import orjson
import pytest
from confluent_kafka import KafkaError

from fakes import FakeConsumer, FakeProducer, messages
from telemetry_processor.benchmark import make_detection, make_envelope
from telemetry_processor.config import Config
from telemetry_processor.metrics import Stats, render
from telemetry_processor.service import Service

IN, OUT, REJ = "iot.mqtt.ingest", "iot.detections", "iot.detections.rejected"


def valid(tunnel="T000001", position="start", seq=1) -> bytes:
    return orjson.dumps(make_envelope(make_detection(tunnel, position, seq)))


def invalid(tunnel="T000001") -> bytes:
    return orjson.dumps(make_envelope(make_detection(tunnel), cn="intruder"))


@pytest.fixture
def svc():
    events: list = []
    holder = {}

    def producer_factory(conf):
        holder["producer"] = FakeProducer(conf)
        holder["producer"].events = events
        return holder["producer"]

    def consumer_factory(conf):
        holder["consumer"] = FakeConsumer(conf, events)
        return holder["consumer"]

    cfg = Config.model_validate({"kafka": {"commit_interval_s": 0}})
    service = Service(cfg, Stats(), None, consumer_factory=consumer_factory, producer_factory=producer_factory)
    service.consumer.subscribe([IN], service._on_assign, service._on_revoke, service._on_lost)
    service.consumer.assign(service, range(6))
    service.producer_ok = True
    return service


def step(service):
    service._collect()
    service._commit()


# --- routing ---------------------------------------------------------------------

def test_accepted_keyed_by_tunnel_and_rejected_to_rejected_topic(svc):
    svc._process_batch(messages([valid("T000007"), invalid("T000008"), b"junk"]))
    svc.producer.deliver()
    out, rej = svc.producer.records(OUT), svc.producer.records(REJ)
    assert [(k, r["tunnel_id"]) for k, r, _ in out] == [("T000007", "T000007")]
    assert [(k, r["reason"]) for k, r, _ in rej] == [("T000008", "identity_mismatch"), (None, "bad_envelope")]
    assert all(h is None for _, _, h in out + rej)
    step(svc)
    s = svc.stats
    assert (s.consumed, s.produced, s.rejected_produced, s.rejected["identity_mismatch"], s.rejected["bad_envelope"]) \
        == (3, 1, 2, 1, 1)
    assert sum(s.event_age.counts) == 1 and sum(s.batch_duration.counts) == 1 and sum(s.batch_ack.counts) == 1


def test_incoming_trace_headers_pass_through_when_tracing_off(svc):
    tp = b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    svc._process_batch(messages([valid()], headers=[("traceparent", tp), ("x-other", b"1"), ("tracestate", b"a=b")]))
    svc.producer.deliver()
    assert svc.producer.records(OUT)[0][2] == [("traceparent", tp), ("tracestate", b"a=b")]


# --- at-least-once commits ---------------------------------------------------------

def test_offsets_committed_only_after_every_record_of_batch_acknowledged(svc):
    svc._process_batch(messages([valid(seq=1), invalid(), valid(seq=2)], partition=2, start_offset=10))
    svc.producer.deliver(2)
    step(svc)
    assert svc.consumer.commits == []  # one record (accepted or rejected) still unacknowledged
    svc.producer.deliver()
    step(svc)
    assert svc.consumer.commits == [{2: 13}]
    assert svc.committed == {2: 13}


def test_batches_commit_in_poll_order(svc):
    svc._process_batch(messages([valid(seq=1)], partition=0, start_offset=0))   # batch 1
    svc._process_batch(messages([valid(seq=2)], partition=0, start_offset=1)    # batch 2
                       + messages([invalid()], partition=1, start_offset=5))
    svc.producer.deliver(index=1)  # batch 2 fully acknowledged first
    svc.producer.deliver(index=1)
    step(svc)
    assert svc.consumer.commits == []  # batch 1 still pending: nothing may be committed
    svc.producer.deliver()
    step(svc)
    assert svc.consumer.commits == [{0: 2, 1: 6}]


def test_delivery_failure_stops_without_committing_failed_or_later_batches(svc):
    svc._process_batch(messages([valid(seq=1)], partition=0, start_offset=0))
    svc._process_batch(messages([valid(seq=2)], partition=0, start_offset=1))
    svc._process_batch(messages([valid(seq=3)], partition=0, start_offset=2))
    svc.producer.deliver(1)
    svc.producer.deliver(1, err=KafkaError(KafkaError._MSG_TIMED_OUT))
    svc.producer.deliver()
    step(svc)
    assert svc.fatal and "not acknowledged" in svc.fatal
    assert svc.consumer.commits == [{0: 1}]
    assert svc.stats.delivery_failures == 1
    assert svc._shutdown() == 1
    assert svc.consumer.commits == [{0: 1}]
    assert not svc.ready()


def test_commit_failure_is_retried_with_next_commit(svc):
    svc._process_batch(messages([valid()], partition=3, start_offset=7))
    svc.producer.deliver()
    svc.consumer.commit_error = KafkaError(KafkaError.REBALANCE_IN_PROGRESS)
    step(svc)
    assert svc.stats.commit_failures == 1 and svc.uncommitted == {3: 8}
    step(svc)
    assert svc.consumer.commits == [{3: 8}] and svc.stats.commits == 1


def test_revoke_flushes_commits_then_forgets_partitions(svc):
    svc._process_batch(messages([valid(seq=1)], partition=1, start_offset=0) + messages([valid(seq=2)], partition=4))
    svc.consumer.revoke(svc, [1])
    assert svc.consumer.events[:2] == ["flush", "commit"]
    assert svc.consumer.commits == [{1: 1, 4: 1}]
    assert 1 not in svc.assigned and 4 in svc.assigned and svc.joined


def test_revoke_with_unacknowledged_records_drops_their_offsets(svc):
    svc.producer.flush_delivers = False
    svc._process_batch(messages([valid(seq=1)], partition=1) + messages([valid(seq=2)], partition=4))
    svc.consumer.revoke(svc, [1])
    assert svc.consumer.commits == []
    svc.producer.deliver()
    step(svc)
    assert svc.consumer.commits == [{4: 1}]  # the revoked partition is never committed by us


def test_lost_partitions_are_not_committed(svc):
    svc._process_batch(messages([valid()], partition=5))
    svc.consumer.lose(svc, range(6))
    svc.producer.deliver()
    step(svc)
    assert svc.consumer.commits == [] and not svc.assigned and not svc.joined and not svc.ready()


def test_backpressure_retries_when_producer_queue_full(svc):
    svc.producer.buffer_errors = 3
    svc._process_batch(messages([valid()]))
    svc.producer.deliver()
    step(svc)
    assert len(svc.producer.records(OUT)) == 1 and svc.consumer.commits == [{0: 1}]


def test_record_refused_by_producer_becomes_rejection(svc):
    svc.producer.refuse_topic = OUT
    svc._process_batch(messages([valid("T000003")]))
    svc.producer.deliver()
    step(svc)
    (key, rec, _), = svc.producer.records(REJ)
    assert key == "T000003" and rec["reason"] == "bad_payload" and "refused" in rec["detail"]
    assert svc.consumer.commits == [{0: 1}] and svc.stats.produced == 0


def test_sigterm_path_flushes_commits_and_closes(svc):
    svc.producer.flush_delivers = True

    def last_batch():
        svc.request_stop("received SIGTERM")
        return messages([valid(seq=9), invalid()], partition=2, start_offset=40)

    svc.consumer.script = [messages([valid(seq=1)], partition=2, start_offset=39), last_batch]
    svc.consumer.commits.clear()
    svc.consumer.events.clear()
    assert svc.run() == 0
    assert svc.consumer.events[-3:] == ["flush", "commit", "close"]
    assert svc.committed == {2: 42}
    assert not svc.producer.pending and not svc.ready()


def test_readiness_and_metrics(svc):
    assert svc.ready()
    svc._on_client_error("producer", KafkaError(KafkaError._ALL_BROKERS_DOWN))
    assert not svc.ready()
    svc._process_batch(messages([valid()]))
    svc.producer.deliver()
    step(svc)
    assert svc.ready()  # a successful acknowledgement proves the producer again
    text = render(svc.stats, assigned_partitions=len(svc.assigned), inflight_records=0, pending_batches=0,
                  ready=svc.ready())
    for line in ("telemetry_processor_consumed_total 1", "telemetry_processor_produced_total 1",
                 'telemetry_processor_rejected_total{reason="bad_topic"} 0', "telemetry_processor_commit_failures_total 0",
                 'telemetry_processor_batch_duration_seconds_bucket{le="+Inf"} 1',
                 "telemetry_processor_event_age_seconds_count 1", "telemetry_processor_assigned_partitions 6",
                 "telemetry_processor_ready 1"):
        assert line in text.splitlines(), line


def test_fatal_client_error_stops(svc):
    err = KafkaError(KafkaError._FATAL, "fenced", fatal=True)
    svc._on_client_error("producer", err)
    assert svc.fatal and not svc.ready()
