import orjson
import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from fakes import FakeConsumer, FakeProducer, messages
from telemetry_processor.benchmark import make_detection, make_envelope, make_health
from telemetry_processor.config import Config
from telemetry_processor.metrics import Stats
from telemetry_processor.service import Service
from telemetry_processor.tracing import Tracing, _root_rule, enabled_by_env, passthrough_headers

PARENT_TRACE = "0af7651916cd43dd8448eb211c80319c"
SAMPLED_PARENT = f"00-{PARENT_TRACE}-b7ad6b7169203331-01".encode()
UNSAMPLED_PARENT = f"00-{PARENT_TRACE}-b7ad6b7169203331-00".encode()


def make_service(sampler="parentbased_traceidratio", arg="1.0"):
    exporter = InMemorySpanExporter()
    env = {"OTEL_TRACES_SAMPLER": sampler, "OTEL_TRACES_SAMPLER_ARG": arg}
    tracing = Tracing(input_topic="iot.mqtt.ingest", group_id="telemetry-processor", exporter=exporter, environ=env)
    svc = Service(Config(), Stats(), tracing, consumer_factory=FakeConsumer, producer_factory=FakeProducer)
    return svc, exporter


def envelope(**kw) -> bytes:
    return orjson.dumps(make_envelope(make_detection("T000005", "end", 77), **kw))


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    # Samplers come from the environ passed to Tracing; os.environ must not interfere.
    monkeypatch.delenv("OTEL_TRACES_SAMPLER", raising=False)
    monkeypatch.delenv("OTEL_TRACES_SAMPLER_ARG", raising=False)


def test_enabled_only_with_non_empty_endpoint():
    assert not enabled_by_env({})
    assert not enabled_by_env({"OTEL_EXPORTER_OTLP_ENDPOINT": ""})
    assert not enabled_by_env({"OTEL_EXPORTER_OTLP_ENDPOINT": "  "})
    assert enabled_by_env({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector.monitoring.svc:4318"})
    assert not enabled_by_env({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://x:4318", "OTEL_SDK_DISABLED": "true"})


def test_root_rule_matches_standard_samplers():
    assert _root_rule({}) == (True, 1 << 64)
    assert _root_rule({"OTEL_TRACES_SAMPLER": "parentbased_traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0.01"}) \
        == (True, round(0.01 * (1 << 64)))
    assert _root_rule({"OTEL_TRACES_SAMPLER": "traceidratio", "OTEL_TRACES_SAMPLER_ARG": "bogus"}) == (False, 1 << 64)
    assert _root_rule({"OTEL_TRACES_SAMPLER": "always_off"}) == (False, 0)
    assert _root_rule({"OTEL_TRACES_SAMPLER": "jaeger_remote"}) == (False, None)


def test_passthrough_headers():
    assert passthrough_headers(None) is None
    assert passthrough_headers([("a", b"1")]) is None
    assert passthrough_headers([("tracestate", b"x=1"), ("a", b"1")]) == [("tracestate", b"x=1")]


def test_sampled_root_span_injected_into_produced_record():
    svc, exporter = make_service()
    svc._process_batch(messages([envelope()], partition=3, start_offset=99))
    assert exporter.get_finished_spans() == ()  # span ends on delivery
    svc.producer.deliver()
    (span,) = exporter.get_finished_spans()
    (_, rec, headers), = svc.producer.records("iot.detections")
    ctx = span.get_span_context()
    assert headers == [("traceparent", f"00-{ctx.trace_id:032x}-{ctx.span_id:016x}-{ctx.trace_flags:02x}".encode())]
    assert span.parent is None and span.name == "process iot.mqtt.ingest"
    a = span.attributes
    assert (a["messaging.system"], a["messaging.destination.partition.id"], a["messaging.kafka.offset"]) == (
        "kafka", "3", 99)
    assert (a["iot.outcome"], a["iot.tunnel_id"], a["iot.message_id"]) == ("accepted", "T000005", rec["message_id"])
    assert svc.stats.traced == 1


def test_incoming_sampled_traceparent_is_continued():
    svc, exporter = make_service(arg="0")  # root sampling off: only the parent decides
    svc._process_batch(messages([envelope(cn="T1")], headers=[("traceparent", SAMPLED_PARENT)]))
    svc.producer.deliver()
    (span,) = exporter.get_finished_spans()
    assert f"{span.context.trace_id:032x}" == PARENT_TRACE
    assert f"{span.parent.span_id:016x}" == "b7ad6b7169203331"
    assert span.attributes["iot.reject_reason"] == "identity_mismatch"
    (_, rec, headers), = svc.producer.records("iot.detections.rejected")
    traceparent = headers[0][1].decode()
    assert traceparent.startswith(f"00-{PARENT_TRACE}-") and int(traceparent[-2:], 16) & 1  # sampled
    assert f"{span.context.span_id:016x}" in traceparent


def test_unsampled_parent_passes_through_without_span():
    svc, exporter = make_service()
    hdrs = [("traceparent", UNSAMPLED_PARENT), ("tracestate", b"v=1")]
    svc._process_batch(messages([envelope()], headers=hdrs))
    svc.producer.deliver()
    assert exporter.get_finished_spans() == ()
    assert svc.producer.records("iot.detections")[0][2] == hdrs


def test_unsampled_root_adds_no_headers_and_no_spans():
    svc, exporter = make_service(arg="0")
    svc._process_batch(messages([envelope()] * 50))
    svc.producer.deliver()
    assert exporter.get_finished_spans() == ()
    assert all(h is None for _, _, h in svc.producer.records("iot.detections"))


def test_ratio_sampling_is_consistent_with_sdk():
    svc, exporter = make_service(arg="0.1")
    chosen = []
    start = svc.tracing._start
    svc.tracing._start = lambda *a: chosen.append(1) or start(*a)
    n = 5000
    svc._process_batch(messages([envelope()] * n))
    svc.producer.deliver()
    spans = exporter.get_finished_spans()
    # every record the fast path chose was also sampled by the SDK sampler (same trace id rule)
    assert len(chosen) == len(spans) == svc.stats.traced
    assert 0.07 * n < len(spans) < 0.13 * n
    bound = round(0.1 * (1 << 64))
    assert all((s.context.trace_id & ((1 << 64) - 1)) < bound for s in spans)
    headers = [h for _, _, h in svc.producer.records("iot.detections") if h]
    assert len(headers) == len(spans)


def test_delivery_failure_marks_span_error():
    from confluent_kafka import KafkaError
    svc, exporter = make_service(sampler="always_on")
    svc._process_batch(messages([envelope()]))
    svc.producer.deliver(err=KafkaError(KafkaError._MSG_TIMED_OUT))
    (span,) = exporter.get_finished_spans()
    assert span.status.status_code.name == "ERROR"


def test_a_traced_health_report_does_not_crash_on_the_missing_message_id():
    """Health payloads have no message_id; annotating one used to raise KeyError and kill the service."""
    svc, exporter = make_service()
    health = orjson.dumps(make_envelope(make_health("T000005", "end"),
                                        topic="tunnels/T000005/sensors/end/health"))
    svc._process_batch(messages([health]))
    svc.producer.deliver()
    (span,) = exporter.get_finished_spans()
    a = span.attributes
    assert (a["iot.outcome"], a["iot.tunnel_id"], a["iot.sensor_id"]) == ("accepted", "T000005", "T000005-end")
    assert "iot.message_id" not in a
    assert svc.producer.records("iot.sensor-health")
