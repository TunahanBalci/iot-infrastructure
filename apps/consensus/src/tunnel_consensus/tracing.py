"""OpenTelemetry tracing for Kafka mode (W3C traceparent in record headers).

Off unless OTEL_EXPORTER_OTLP_ENDPOINT is set to a non-empty value; the SDK is then not even
imported. When on, only records whose traceparent has the sampled flag cost anything: the
header is kept on the detection, and the vehicle event fused from it gets a span (child of
the first sampled detection, linked to the others) whose context is injected into the
produced record. Sampling itself is decided upstream (OTEL_TRACES_SAMPLER=parentbased_*).
"""

from __future__ import annotations

import logging
import os
import time

from .model import Detection

log = logging.getLogger(__name__)

TRACEPARENT = "traceparent"
_ODD_HEX = frozenset(b"13579bdfBDF")   # last hex digit of the flags: bit 0 = sampled


def sampled_traceparent(headers: list[tuple[str, bytes]] | None) -> bytes | None:
    """The traceparent header value if present and sampled, else None."""
    if headers:
        for key, value in headers:
            if key == TRACEPARENT and value and len(value) >= 55 and value[-1] in _ODD_HEX:
                return value
    return None


def tracing_enabled(environ: dict[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    if env.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return False
    return bool(env.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip())


class Tracing:
    """Span around fusion of sampled vehicle events; injects the span context into the output record."""

    def __init__(self, vehicles_topic: str):
        # Imported lazily: tracing is optional and the SDK import costs startup time.
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

        resource = Resource.create({"service.name": os.environ.get("OTEL_SERVICE_NAME") or "consensus"})
        # Sampler from OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG, endpoint from OTEL_EXPORTER_OTLP_*.
        self.provider = TracerProvider(resource=resource)
        self.provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        self.tracer = self.provider.get_tracer("tunnel_consensus")
        self.trace = trace
        self.propagator = TraceContextTextMapPropagator()
        self.vehicles_topic = vehicles_topic
        self.record_start_ns = 0   # set by the consumer loop before each record
        log.info("tracing on: OTLP endpoint %s", os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"))

    def vehicle_headers(self, event: dict, dets: list[Detection]) -> list[tuple[str, bytes]] | None:
        parents = [d.trace for d in dets if d.trace is not None]
        if not parents:
            return None
        trace, prop = self.trace, self.propagator
        ctx = prop.extract({TRACEPARENT: parents[0].decode()})
        links = []
        for tp in parents[1:]:
            sc = trace.get_current_span(prop.extract({TRACEPARENT: tp.decode()})).get_span_context()
            if sc.is_valid:
                links.append(trace.Link(sc))
        span = self.tracer.start_span(
            "consensus fuse", context=ctx, kind=trace.SpanKind.PRODUCER, links=links,
            start_time=self.record_start_ns or time.time_ns(),
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": self.vehicles_topic,
                "messaging.kafka.message.key": event["tunnel_id"],
                "tunnel.id": event["tunnel_id"],
                "vehicle.event_id": event["event_id"],
                "vehicle.sensors": event["sensors"],
                "vehicle.classification": event["classification"],
            })
        carrier: dict[str, str] = {}
        prop.inject(carrier, context=trace.set_span_in_context(span))
        span.end()
        value = carrier.get(TRACEPARENT)
        return [(TRACEPARENT, value.encode())] if value else None

    def shutdown(self) -> None:
        try:
            self.provider.shutdown()
        except Exception:  # best effort on exit
            log.exception("tracing shutdown failed")
