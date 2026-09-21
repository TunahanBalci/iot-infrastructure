"""Kafka mode without a broker: config, watermarks, replay/revoke semantics, geometry, output schemas."""

from collections import Counter, defaultdict

import orjson
import pytest

pytest.importorskip("confluent_kafka")
from confluent_kafka import KafkaError, TopicPartition  # noqa: E402

from tunnel_consensus.config import Config, apply_env_overrides, load_config  # noqa: E402
from tunnel_consensus.engine import ConsensusEngine  # noqa: E402
from tunnel_consensus.kafka_io import (  # noqa: E402
    GeometryTopic,
    KafkaMetrics,
    KafkaSink,
    consumer_config,
    geometry_value,
    parse_geometry,
    producer_config,
)
from tunnel_consensus.kafka_service import KafkaService, commit_metadata, parse_commit_metadata  # noqa: E402
from tunnel_consensus.tracing import sampled_traceparent, tracing_enabled  # noqa: E402

T0 = 1_800_000_000_000


def detection(seq=1, position="start", ts=T0, direction="start_to_end", tunnel="T000001", **kw) -> bytes:
    body = {"schema": 2, "message_id": f"{tunnel}-{position}:boot:{seq}", "sensor_id": f"{tunnel}-{position}",
            "device_id": "02:aa:bb:cc:dd:ee", "device_serial": "TSN-QUT73ELQA5",
            "tunnel_id": tunnel, "position": position, "seq": seq, "ts": ts, "direction": direction,
            # one plate per vehicle: the three sensors of a vehicle read the same one
            "lane": 1, "plate": "34 ABC 123", "speed_kmh": 72.0, "length_m": 4.5,
            "occupancy_ms": 225, "classification": "OTOMOBIL", "confidence": 0.95,
            "received_ts": ts + 5, "processed_ts": ts + 6, "tbmq_node": "tbmq-0"}
    body.update(kw)
    return orjson.dumps(body)


# --- fakes -----------------------------------------------------------------------

class FakeProducer:
    def __init__(self, log=None):
        self.records = []
        self.log = log if log is not None else []

    def produce(self, topic, value=None, key=None, headers=None):
        self.records.append((topic, key, value, headers))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        self.log.append(("flush", len(self.records)))
        return 0

    def __len__(self):
        return 0

    def by_topic(self, topic):
        return [(k, None if v is None else orjson.loads(v)) for t, k, v, _ in self.records if t == topic]


class FakeConsumer:
    def __init__(self, log, committed=None):
        self.log = log
        self.commits = []
        self._committed = committed or {}

    def commit(self, offsets=None, asynchronous=True):
        self.log.append(("commit", [(tp.partition, tp.offset) for tp in offsets]))
        self.commits.append(offsets)
        return offsets

    def committed(self, partitions, timeout=None):
        out = []
        for tp in partitions:
            offset, meta = self._committed.get(tp.partition, (-1001, None))
            out.append(TopicPartition(tp.topic, tp.partition, offset, **({"metadata": meta} if meta else {})))
        return out

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return 0, 1_000_000


class FakeGeometry:
    def __init__(self, halves=None):
        self.halves = dict(halves or {})
        self.loaded = True
        self.reads = 0
        self.published = []
        self.records_read = self.records_published = 0

    def read_to_end(self, timeout_s):
        self.reads += 1
        return 0

    def publish(self, tunnel_id, half):
        self.published.append((tunnel_id, half))
        self.halves[tunnel_id] = half
        return True


def kafka_cfg(**output) -> Config:
    return Config.model_validate({"input": {"source": "kafka"}, "output": {"sink": "kafka", **output},
                                  "kafka": {"sasl_password": "x"}})


def service(committed=None, halves=None):
    log = []
    producer = FakeProducer(log)
    svc = KafkaService(kafka_cfg(), consumer=None, producer=producer, geometry=FakeGeometry(halves), hostname="pod-1")
    return svc, producer, FakeConsumer(log, committed), log


# --- config ------------------------------------------------------------------------

def test_kafka_defaults_follow_the_contract():
    k = Config().kafka
    assert (k.detections_topic, k.vehicles_topic, k.traffic_topic, k.geometry_topic) == (
        "iot.detections", "iot.vehicles", "iot.traffic", "iot.consensus.geometry")
    assert (k.group_id, k.security_protocol, k.sasl_mechanism, k.sasl_username) == (
        "consensus", "SASL_SSL", "SCRAM-SHA-512", "consensus")
    assert k.bootstrap_servers == "app-kafka-kafka-bootstrap.iot-pipeline.svc:9093"
    assert k.ssl_ca_location == "/etc/app-kafka/ca.crt"
    assert Config().input.source == "mqtt" and Config().output.sink == "mqtt"
    assert load_config("config/consensus.yaml", environ={}) == Config()


def test_kafka_env_overrides_and_passthrough():
    env = {"CONSENSUS__INPUT__SOURCE": "kafka", "CONSENSUS__OUTPUT__SINK": "kafka",
           "CONSENSUS__KAFKA__SASL_PASSWORD": "000123", "CONSENSUS__KAFKA__BOOTSTRAP_SERVERS": "10.0.0.1:9093",
           "CONSENSUS__KAFKA__CONSUMER__SESSION_TIMEOUT_MS": "10000",
           "CONSENSUS__KAFKA__PRODUCER__LINGER_MS": "50"}
    cfg = Config.model_validate(apply_env_overrides({}, env))
    k = cfg.kafka
    assert k.sasl_password == "000123" and k.bootstrap_servers == "10.0.0.1:9093"
    assert k.consumer == {"session.timeout.ms": 10000}
    c = consumer_config(k, "c1")
    assert c["session.timeout.ms"] == 10000 and c["group.id"] == "consensus"
    assert c["partition.assignment.strategy"] == "cooperative-sticky" and c["enable.auto.commit"] is False
    assert c["sasl.password"] == "000123" and c["ssl.ca.location"] == "/etc/app-kafka/ca.crt"
    p = producer_config(k, "c1")
    assert (p["partitioner"], p["acks"], p["enable.idempotence"], p["compression.type"], p["linger.ms"]) == (
        "murmur2_random", "all", True, "lz4", 50)
    assert k.resolved_client_id("consensus-7d9f-abc") == "consensus-consensus-7d9f-abc"


def test_kafka_input_rejects_mqtt_sink_and_missing_password():
    with pytest.raises(ValueError):
        Config.model_validate({"input": {"source": "kafka"}})   # sink defaults to mqtt
    with pytest.raises(ValueError, match="sasl_password"):
        consumer_config(Config().kafka, "c1")
    plain = Config.model_validate({"kafka": {"security_protocol": "PLAINTEXT"}}).kafka
    assert "sasl.username" not in consumer_config(plain, "c1")


def test_commit_metadata_round_trip():
    assert parse_commit_metadata(commit_metadata(1200, 1234, 1_800_000_000.5)) == (1200, 1234, 1_800_000_000.5)
    for bad in (None, "", "x", '{"w": 1, "p": "1", "c": 2}', '{"w": -1, "p": 1, "c": 2}', '{"p": 1, "c": 2}',
                '{"w": 5, "p": 4, "c": 2}'):
        assert parse_commit_metadata(bad) is None


def test_tracing_off_when_endpoint_empty():
    assert not tracing_enabled({})
    assert not tracing_enabled({"OTEL_EXPORTER_OTLP_ENDPOINT": ""})
    assert not tracing_enabled({"OTEL_EXPORTER_OTLP_ENDPOINT": "  "})
    assert tracing_enabled({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector.monitoring.svc:4318"})
    tp = b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    assert sampled_traceparent([("x", b"1"), ("traceparent", tp)]) == tp
    assert sampled_traceparent([("traceparent", tp[:-2] + b"00")]) is None
    assert sampled_traceparent(None) is None


def test_tracing_continues_sampled_traces_into_vehicle_records(monkeypatch):
    pytest.importorskip("opentelemetry.sdk")
    import opentelemetry.exporter.otlp.proto.http.trace_exporter as otlp
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    monkeypatch.setattr(otlp, "OTLPSpanExporter", lambda: exporter)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
    from tunnel_consensus.model import parse_detection
    from tunnel_consensus.tracing import Tracing

    tracing = Tracing("iot.vehicles")
    parent = b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    d1, d2 = parse_detection(detection(1, "start")), parse_detection(detection(1, "end"))
    d1.trace = parent
    event = {"tunnel_id": "T000001", "event_id": d1.message_id, "sensors": 2, "classification": "OTOMOBIL"}
    ((key, value),) = tracing.vehicle_headers(event, [d1, d2])
    version, trace_id, span_id, flags = value.decode().split("-")
    assert key == "traceparent" and trace_id == "4bf92f3577b34da6a3ce929d0e0e4736" and flags == "01"
    assert span_id != "00f067aa0ba902b7"                       # our fusion span, child of the detection's
    assert tracing.vehicle_headers(event, [d2]) is None        # nothing sampled: no header, no span
    tracing.provider.shutdown()
    (span,) = exporter.get_finished_spans()
    assert span.name == "consensus fuse" and span.attributes["tunnel.id"] == "T000001"
    assert format(span.parent.span_id, "016x") == "00f067aa0ba902b7"


# --- watermark -----------------------------------------------------------------------

def tracked_engine(sink=None, halves=None):
    out = []

    class ListSink:
        def vehicle(self, tunnel_id, payload, event, dets):
            out.append(("vehicle", tunnel_id, event))

        def traffic(self, tunnel_id, payload):
            out.append(("traffic", tunnel_id, orjson.loads(payload)))

    engine = ConsensusEngine(Config(), sink=sink or ListSink(), known_halves={"T000001": 500.0} if halves is None
                             else halves, track_offsets=True)
    return engine, out


def test_watermark_is_oldest_pending_detection():
    engine, out = tracked_engine()
    assert engine.watermark() == -1
    engine.ingest(detection(1, "start", T0), offset=10)
    assert engine.watermark() == 10 and engine.next_offset == 11
    engine.ingest(detection(1, "end", T0 + 1_000, lane=2), offset=11)       # another vehicle, emitted alone
    engine.ingest(b"not json", offset=12)
    engine.ingest(detection(2, "start", T0 + 2_000, lane=3), offset=13)     # pending, later offset
    assert len(out) == 1 and engine.watermark() == 10
    engine.ingest(detection(2, "middle", T0 + 25_000), offset=14)
    engine.ingest(detection(3, "end", T0 + 50_000), offset=15)              # completes the first vehicle
    assert out[-1][2]["sensors"] == 3
    assert engine.watermark() == 13                                         # lane 3 vehicle still pending
    engine.ingest(detection(4, "end", T0 + 52_000, lane=3), offset=16)
    assert engine.stats.pending == 0 and engine.watermark() == 17


def test_replay_start_covers_whole_vehicles_across_the_watermark():
    engine, out = tracked_engine()
    engine.ingest(detection(1, "start", T0), offset=10)                     # vehicle 1
    engine.ingest(detection(2, "start", T0 + 20_000, lane=2), offset=11)    # vehicle 2, stays pending
    engine.ingest(detection(1, "middle", T0 + 25_000), offset=12)
    engine.ingest(detection(1, "end", T0 + 50_000), offset=13)              # vehicle 1 emitted
    assert engine.watermark() == 11
    assert engine.replay_start(engine.watermark()) == 10    # vehicle 1 has detections on both sides of 11
    pending_from, position, clock = engine.replay_boundary()
    assert (pending_from, position, clock) == (11, 14, T0 / 1000 + 50)


def test_end_of_replay_drops_what_the_previous_owner_emitted_and_keeps_the_rest():
    engine, out = tracked_engine()
    # Previous owner: position 3, event clock T0 + 120 s. The vehicle at T0 had to leave by ~T0 + 55 s
    # (it was emitted as a single then); the one at T0 + 100 s was still under way.
    engine.begin_replay(3, T0 / 1000 + 120, pending_from=0)
    engine.ingest(detection(1, "start", T0), offset=0)
    engine.ingest(detection(2, "start", T0 + 100_000, lane=2), offset=1)
    engine.ingest(detection(3, "start", T0 + 101_000, lane=3), offset=2)
    assert out == [] and engine.stats.pending == 3            # replay: nothing expires, nothing emitted
    engine.ingest(detection(4, "end", T0 + 121_000, lane=4), offset=3)   # first record past the position
    assert engine.replay_until is None and engine.stats.replay_dropped == 1
    assert engine.clock == T0 / 1000 + 121 and engine.stats.pending == 2
    assert [e["sensors"] for _, _, e in out] == [1]           # the new single at offset 3 only
    assert engine.watermark() == 1


def test_calibrating_and_duplicate_detections_do_not_hold_the_watermark():
    engine, _ = tracked_engine(halves={})
    engine.ingest(detection(1, "start", T0), offset=0)
    engine.ingest(detection(1, "start", T0), offset=1)      # duplicate
    assert engine.stats.calibrating == 1 and engine.stats.duplicates == 1
    assert engine.watermark() == 2


# --- rebalance: revoke and assign ------------------------------------------------------

def test_revoke_flushes_commits_watermark_then_drops_state_without_emitting():
    svc, producer, consumer, log = service(halves={"T000001": 500.0, "T000002": 500.0})
    svc.on_assign(consumer, [TopicPartition("iot.detections", 0), TopicPartition("iot.detections", 1)])
    assert sorted(svc.engines) == [0, 1] and svc.ready()
    e0 = svc.engines[0]
    e0.ingest(detection(1, "start", T0), offset=100)
    e0.ingest(detection(1, "middle", T0 + 25_000), offset=101)
    svc.engines[1].ingest(detection(1, "start", T0, tunnel="T000002"), offset=7)
    assert e0.stats.pending == 2 and producer.by_topic("iot.vehicles") == []

    svc.on_revoke(consumer, [TopicPartition("iot.detections", 0)])
    assert [entry[0] for entry in log] == ["flush", "commit"]            # flush before commit
    (tp,) = consumer.commits[0]
    assert (tp.partition, tp.offset) == (0, 100)                          # oldest pending, not the position
    assert parse_commit_metadata(tp.metadata) == (100, 102, T0 / 1000 + 25)   # pending from, position, clock
    assert list(svc.engines) == [1]
    assert producer.by_topic("iot.vehicles") == []                        # no partial events
    assert svc.revoked_pending == 2 and svc.totals()["received"] == 3


def test_commit_skips_unchanged_and_untouched_partitions():
    svc, producer, consumer, log = service()
    svc.on_assign(consumer, [TopicPartition("iot.detections", 0), TopicPartition("iot.detections", 1)])
    svc.engines[0].ingest(detection(1, "end", T0), offset=5)
    assert svc._commit([0, 1], consumer)
    assert [(tp.partition, tp.offset) for tp in consumer.commits[0]] == [(0, 6)]
    assert svc._commit([0, 1], consumer) and len(consumer.commits) == 1


def test_delivery_failure_blocks_commits():
    svc, producer, consumer, log = service()
    svc.on_assign(consumer, [TopicPartition("iot.detections", 0)])
    svc.engines[0].ingest(detection(1, "end", T0), offset=5)

    class Msg:
        def topic(self):
            return "iot.vehicles"

    svc.failures(KafkaError(KafkaError._MSG_TIMED_OUT), Msg())
    assert not svc._commit([0], consumer)
    assert consumer.commits == [] and svc.fatal and not svc.ready() and not svc.healthy()


def test_assign_seeds_geometry_and_starts_replay_from_commit_metadata():
    committed = {3: (100, commit_metadata(120, 150, T0 / 1000 + 60)), 4: (40, None)}
    svc, producer, consumer, _ = service(committed, halves={"T000001": 500.0})
    svc.on_assign(consumer, [TopicPartition("iot.detections", 3), TopicPartition("iot.detections", 4)])
    assert svc.geometry.reads == 1
    e3, e4 = svc.engines[3], svc.engines[4]
    assert (e3.replay_until, e3.replay_pending_from, e3.replay_clock) == (150, 120, T0 / 1000 + 60)
    assert e4.replay_until is None
    e3.ingest(detection(1, "start", T0), offset=100)
    assert e3.stats.calibrating == 0 and e3.tunnels["T000001"].half == 500.0   # seeded, no recalibration
    # A later geometry record reaches tunnels that are created afterwards.
    svc.geometry.halves["T000009"] = 800.0
    e4.ingest(detection(1, "start", T0, tunnel="T000009"), offset=40)
    assert e4.tunnels["T000009"].half == 800.0


def test_new_calibration_is_published_before_the_next_commit():
    svc, producer, consumer, log = service(halves={})
    svc.on_assign(consumer, [TopicPartition("iot.detections", 0)])
    svc.engines[0].on_calibrated("T000005", 612.5)
    svc._publish_calibrated()
    assert svc.geometry.published == [("T000005", 612.5)]


# --- replay semantics against the simulator -----------------------------------------------

def _vehicles(events, dets_by_id):
    """event_id -> set of ground-truth vehicle ids."""
    return {e["event_id"]: {dets_by_id[m]["vehicle_id"] for m in e["detections"]} for e in events}


def _run(engine, payloads, start, stop, clear=None):
    for offset in range(start, stop):
        engine.ingest(payloads[offset], offset=offset)


class EventList:
    def __init__(self):
        self.events = []

    def vehicle(self, tunnel_id, payload, event, dets):
        self.events.append(event)

    def traffic(self, tunnel_id, payload):
        pass


def handover(payloads, cuts, crash_extra=0, replay=True):
    """Owner i processes up to cuts[i] (+crash_extra records it emits but never commits), commits,
    and owner i+1 resumes from the committed offset. Returns all emitted events.
    replay=False: the new owner resumes at the pending watermark and emits everything."""
    cfg = Config()
    events = []
    start, boundary, halves = 0, None, {}
    for i, cut in enumerate([*cuts, len(payloads)]):
        sink = EventList()
        eng = ConsensusEngine(cfg, sink=sink, known_halves=dict(halves), track_offsets=True)
        if replay and boundary is not None and boundary[1] > start:
            eng.begin_replay(boundary[1], boundary[2], boundary[0])
        last = i == len(cuts)
        _run(eng, payloads, start, cut)
        boundary = eng.replay_boundary()
        watermark = eng.replay_start(boundary[0]) if replay else eng.watermark()
        halves.update(eng.learned_halves())
        if last:
            eng.end_replay()
            eng.flush()
        else:
            _run(eng, payloads, cut, min(len(payloads), cut + crash_extra))   # crashed before committing these
        events += sink.events
        start = watermark
    return events


@pytest.fixture(scope="module")
def sim_payloads():
    pytest.importorskip("tunnel_sim")
    from simdata import simulate

    return simulate(900, tunnels=12)


def test_handover_loses_nothing_and_duplicates_keep_their_event_id(sim_payloads):
    payloads = sim_payloads
    dets = {d["message_id"]: d for d in map(orjson.loads, payloads)}
    n = len(payloads)
    baseline = handover(payloads, [])
    # Evaluate vehicles the baseline emitted after calibration (first ~3 simulated minutes).
    from_ts = min(d["ts"] for d in dets.values()) + 300_000
    base_vehicles = {v for e in baseline if e["ts_entry"] >= from_ts for v in _vehicles([e], dets)[e["event_id"]]}
    assert len(base_vehicles) > 2000

    for cuts, crash_extra in (([n // 2], 0), ([n // 3, 2 * n // 3], 0), ([n // 2], 2000)):
        events = handover(payloads, cuts, crash_extra)
        ids = Counter(e["event_id"] for e in events)
        covered = defaultdict(set)
        for e in events:
            for v in _vehicles([e], dets)[e["event_id"]]:
                covered[v].add(e["event_id"])
        lost = base_vehicles - covered.keys()
        duplicated_ids = sum(c - 1 for c in ids.values() if c > 1)
        base_ids = defaultdict(set)
        for e in baseline:
            for v in _vehicles([e], dets)[e["event_id"]]:
                base_ids[v].add(e["event_id"])
        split = sum(1 for v in base_vehicles if len(covered[v] - base_ids[v]) > 0)
        print(f"cuts={cuts} crash_extra={crash_extra}: events={len(events)} baseline={len(baseline)} "
              f"lost={len(lost)} same-id duplicates={duplicated_ids} vehicles with a different event id={split}")
        assert not lost
        if crash_extra == 0:
            # Re-emitted across the boundary: same event_id, so ClickHouse replaces them
            # (ReplacingMergeTree on (tunnel_id, event_id)). What must not happen is a vehicle
            # being lost, or coming back under a different id.
            assert duplicated_ids <= 50 * len(cuts)
        assert split <= 5 * len(cuts)

    naive = handover(payloads, [n // 2], replay=False)
    naive_dups = sum(c - 1 for c in Counter(e["event_id"] for e in naive).values() if c > 1)
    assert naive_dups > 50     # without replay suppression the new owner re-emits minutes of vehicles


# --- outputs -------------------------------------------------------------------------------

def test_kafka_outputs_are_keyed_and_the_traffic_report_carries_its_ts():
    cfg = kafka_cfg()
    producer, metrics = FakeProducer(), KafkaMetrics()
    engine = ConsensusEngine(cfg, sink=KafkaSink(cfg, producer, metrics), known_halves={"T000001": 500.0},
                             track_offsets=True)
    engine.ingest(detection(1, "start", T0), offset=0)
    engine.ingest(detection(1, "middle", T0 + 25_000), offset=1)
    engine.ingest(detection(1, "end", T0 + 50_000), offset=2)
    engine.report_traffic(60.0, now_ms=T0 + 60_000)

    (key, v), = producer.by_topic("iot.vehicles")
    assert key == "T000001"
    assert set(v) == {"schema", "event_id", "tunnel_id", "direction", "lane", "ts_entry", "ts_exit", "speed_kmh",
                      "length_m", "plate", "classification", "confidence", "agreement", "sensors", "missing",
                      "detections"}
    traffic = producer.by_topic("iot.traffic")
    assert not producer.by_topic("iot.sensor-health")   # the devices report their own health
    assert [k for k, _ in traffic] == ["T000001"]
    t = traffic[0][1]
    assert list(t) == ["schema", "tunnel_id", "ts", "window_s", "length_m", "vehicles", "counts",
                       "avg_speed_kmh"]
    assert t["ts"] == T0 + 60_000 and t["vehicles"] == 1
    assert t["counts"]["start_to_end"] == {"OTOMOBIL": 1}
    assert all(headers is None for *_, headers in producer.records)
    assert sum(metrics.age_counts) == 1
    assert 'consensus_event_age_seconds_bucket{le="+Inf"} 1' in "\n".join(metrics.histogram_lines(
        "consensus_event_age_seconds"))


def test_metrics_text_has_kafka_series():
    svc, producer, consumer, _ = service(halves={"T000001": 500.0})
    svc.on_assign(consumer, [TopicPartition("iot.detections", 0)])
    svc.engines[0].ingest(detection(1, "end", T0), offset=0)
    svc.refresh_stats(consumer, log_line=True)
    text = svc.metrics_text
    for name in ("consensus_received_total 1", "consensus_vehicles_total 1", "consensus_kafka_assigned_partitions 1",
                 "consensus_kafka_rebalances_total 1", "consensus_kafka_commit_failures_total 0",
                 "consensus_kafka_produce_errors_total 0", "consensus_geometry_loaded_tunnels 1",
                 "consensus_event_age_seconds_count 1", "consensus_ready 1"):
        assert name in text, name


# --- geometry topic ---------------------------------------------------------------------------

class GeoMsg:
    def __init__(self, partition, offset, key=None, value=None, eof=False):
        self._p, self._o, self._k, self._v, self._eof = partition, offset, key, value, eof

    def error(self):
        return KafkaError(KafkaError._PARTITION_EOF) if self._eof else None

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return self._k

    def value(self):
        return self._v


class GeoConsumer:
    """One-partition compacted topic with a few records, then EOF."""

    def __init__(self, records):
        self.records = records
        self.assigned = None
        self.pos = 0

    def list_topics(self, topic, timeout=None):
        class P:
            error = None

        class T:
            error = None
            partitions = {0: P()}

        class MD:
            topics = {topic: T()}

        return MD()

    def assign(self, tps):
        self.assigned = tps

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return 0, len(self.records)

    def consume(self, n, timeout):
        out = [GeoMsg(0, i, k, v) for i, (k, v) in enumerate(self.records) if i >= self.pos]
        self.pos = len(self.records)
        return out + [GeoMsg(0, len(self.records), eof=True)]

    def close(self):
        pass


def test_geometry_topic_read_to_end_publish_and_tombstones():
    records = [(b"T000001", geometry_value("T000001", 500.0, 1)), (b"T000002", geometry_value("T000002", 250.0, 1)),
               (b"T000002", None), (b"T000003", b"garbage"), (b"T000004", geometry_value("T000004", 600.04, 1))]
    cfg = kafka_cfg()
    producer = FakeProducer()
    geo = GeometryTopic(cfg, "c1", producer, KafkaMetrics(), consumer_factory=lambda conf: GeoConsumer(records))
    assert geo.read_to_end(5) == 4 and geo.loaded
    assert geo.halves == {"T000001": 500.0, "T000004": pytest.approx(600.04)}
    assert not geo.publish("T000001", 500.01)          # same length at 0.1 m resolution
    assert geo.publish("T000001", 510.0)
    assert geo.publish("T000007", 300.0)
    (k1, v1), (k2, v2) = producer.by_topic("iot.consensus.geometry")
    assert (k1, v1["tunnel_id"], v1["length_m"]) == ("T000001", "T000001", 1020.0)
    assert set(v2) == {"tunnel_id", "length_m", "updated_ts"} and isinstance(v2["updated_ts"], int)
    assert parse_geometry(b"T1", None) == ("T1", None) and parse_geometry(None, b"{}") is None


def test_mqtt_input_with_kafka_sink_produces_keyed_records():
    from tunnel_consensus.sharding import Assignment
    from tunnel_consensus.worker import Worker

    cfg = Config.model_validate({"output": {"sink": "kafka"}, "kafka": {
        "security_protocol": "PLAINTEXT", "bootstrap_servers": "127.0.0.1:1", "flush_timeout_s": 0.1,
        "producer": {"message.timeout.ms": 100}}})
    worker = Worker(cfg, Assignment.for_worker(cfg, 0, 0))
    worker.engine.known_halves["T000001"] = 500.0
    worker._on_payload(detection(1, "end", T0))
    assert worker.engine.stats.vehicles == 1 and len(worker.producer) == 1   # queued for iot.vehicles
    assert worker.stats()["out_pending"] == 1
    worker.producer.purge()
    worker.producer.flush(0)
