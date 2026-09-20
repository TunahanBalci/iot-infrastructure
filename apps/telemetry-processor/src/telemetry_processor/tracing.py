"""OpenTelemetry tracing: one span per sampled record, W3C trace context in Kafka record headers.

Off unless OTEL_EXPORTER_OTLP_ENDPOINT is set (an empty value counts as unset); the SDK is only
imported when on. Unsampled records never touch the SDK: the root sampling decision for the
standard samplers (OTEL_TRACES_SAMPLER) is made here on a random trace id with the same rule as
TraceIdRatioBased, and only sampled records create a span (with that trace id, so the SDK sampler
agrees). A traceparent on the input record is continued; if it is not sampled it is passed
through unchanged so downstream services see the same decision.
"""

from __future__ import annotations

import logging
import os
import random
import socket
from collections.abc import Callable
from typing import Any

log = logging.getLogger(__name__)

TRACE_HEADERS = ("traceparent", "tracestate")
_TRACE_ID_LIMIT = (1 << 64) - 1
_getrandbits = random.getrandbits


def enabled_by_env(environ: dict[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    if env.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return False
    return bool(env.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip())


def passthrough_headers(headers: list[tuple[str, bytes]] | None) -> list[tuple[str, bytes]] | None:
    """Trace context headers of an input record, or None."""
    out = None
    if headers:
        for key, value in headers:
            if key == "traceparent" or key == "tracestate":
                if out is None:
                    out = []
                out.append((key, value))
    return out


def _sampler_name(environ: dict[str, str]) -> str:
    return environ.get("OTEL_TRACES_SAMPLER", "").strip().lower() or "parentbased_always_on"


def _sampler_rate(environ: dict[str, str]) -> float:
    try:
        rate = float(environ.get("OTEL_TRACES_SAMPLER_ARG", "1.0"))
        if 0.0 <= rate <= 1.0:
            return rate
    except ValueError:
        pass
    return 1.0  # the SDK does the same


def _root_rule(environ: dict[str, str]) -> tuple[bool, int | None]:
    """(parent_based, bound) for the standard samplers; bound None = let the SDK decide per record."""
    name = _sampler_name(environ)
    parent_based = name.startswith("parentbased_")
    root = name.removeprefix("parentbased_")
    if root == "always_on":
        return parent_based, 1 << 64
    if root == "always_off":
        return parent_based, 0
    if root == "traceidratio":
        return parent_based, round(_sampler_rate(environ) * (1 << 64))  # TraceIdRatioBased.get_bound_for_rate
    return parent_based, None


def _sdk_sampler(environ: dict[str, str]) -> Any:
    """SDK sampler for the standard names (from `environ`, not only os.environ); None = SDK default from env."""
    from opentelemetry.sdk.trace import sampling

    name, rate = _sampler_name(environ), _sampler_rate(environ)
    return {
        "always_on": lambda: sampling.ALWAYS_ON,
        "always_off": lambda: sampling.ALWAYS_OFF,
        "traceidratio": lambda: sampling.TraceIdRatioBased(rate),
        "parentbased_always_on": lambda: sampling.ParentBased(sampling.ALWAYS_ON),
        "parentbased_always_off": lambda: sampling.ParentBased(sampling.ALWAYS_OFF),
        "parentbased_traceidratio": lambda: sampling.ParentBasedTraceIdRatio(rate),
    }.get(name, lambda: None)()


class Tracing:
    def __init__(self, *, input_topic: str, group_id: str, exporter: Any = None,
                 environ: dict[str, str] | None = None):
        """exporter=None: OTLP/HTTP exporter configured from OTEL_* env (batched). Tests pass an
        in-memory exporter (exported synchronously)."""
        from opentelemetry import trace as trace_api
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
        from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

        from . import __version__

        env = os.environ if environ is None else environ

        class PresetTraceIds(RandomIdGenerator):
            """Root spans use the trace id the fast sampling decision was made on."""
            preset = 0

            def generate_trace_id(self) -> int:
                trace_id, self.preset = self.preset, 0
                return trace_id or super().generate_trace_id()

        attributes = {"service.version": __version__, "service.instance.id": socket.gethostname()}
        if not env.get("OTEL_SERVICE_NAME"):
            attributes["service.name"] = "telemetry-processor"
        self._ids = PresetTraceIds()
        self.provider = TracerProvider(resource=Resource.create(attributes), id_generator=self._ids,
                                       sampler=_sdk_sampler(env))
        if exporter is None:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            self.provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        else:
            self.provider.add_span_processor(SimpleSpanProcessor(exporter))
        self._tracer = self.provider.get_tracer("telemetry_processor", __version__)
        self._propagator = TraceContextTextMapPropagator()
        self._set_span_in_context = trace_api.set_span_in_context
        self._consumer_kind = trace_api.SpanKind.CONSUMER
        self._status_error = trace_api.StatusCode.ERROR
        self._parent_based, self._bound = _root_rule(env)
        self.span_name = f"process {input_topic}"
        self._base_attributes = {
            "messaging.system": "kafka",
            "messaging.operation.type": "process",
            "messaging.operation.name": "process",
            "messaging.destination.name": input_topic,
            "messaging.consumer.group.name": group_id,
        }
        log.info("tracing on: sampler=%s exporter=%s", self.provider.sampler.get_description(),
                 type(exporter).__name__ if exporter is not None else "otlp/http")

    # --- per record ---------------------------------------------------------------

    def begin(self, headers: list[tuple[str, bytes]] | None) -> tuple[Any, list[tuple[str, bytes]] | None]:
        """(span, passthrough headers). span is None when the record is not sampled."""
        if headers:
            traceparent = None
            for key, value in headers:
                if key == "traceparent":
                    traceparent = value
            if traceparent is not None:
                return self._continue(headers, traceparent)
        bound = self._bound
        if bound is None:
            return self._start(None, None)
        if bound == 0:
            return None, None
        trace_id = _getrandbits(128)
        if (trace_id & _TRACE_ID_LIMIT) >= bound or trace_id == 0:
            return None, None
        self._ids.preset = trace_id
        return self._start(None, None)

    def _continue(self, headers: list[tuple[str, bytes]], traceparent: bytes) -> tuple[Any, Any]:
        passthrough = passthrough_headers(headers)
        # traceparent = 00-<32 hex trace id>-<16 hex span id>-<2 hex flags>
        if self._parent_based and len(traceparent) == 55 and traceparent[53:55] in (b"00", b"02"):
            return None, passthrough  # parent not sampled (flag bit 0 clear)
        carrier = {k: v.decode("latin-1") for k, v in passthrough if v is not None}
        return self._start(self._propagator.extract(carrier), passthrough)

    def _start(self, context: Any, passthrough: Any) -> tuple[Any, Any]:
        span = self._tracer.start_span(self.span_name, context=context, kind=self._consumer_kind,
                                       attributes=self._base_attributes)
        if span.is_recording():
            return span, None
        return None, passthrough

    def annotate(self, span: Any, partition: int, offset: int, reason: str | None, output_topic: str,
                 info: Any) -> list[tuple[str, bytes]]:
        """Record attributes; returns the headers for the produced record (this span as parent)."""
        span.set_attribute("messaging.destination.partition.id", str(partition))
        span.set_attribute("messaging.kafka.offset", offset)
        span.set_attribute("iot.output_topic", output_topic)
        if reason is None:
            span.set_attribute("iot.outcome", "accepted")
            span.set_attribute("iot.tunnel_id", info["tunnel_id"])
            span.set_attribute("iot.sensor_id", info["sensor_id"])
            # Health reports carry no message_id; only detections do.
            if (message_id := info.get("message_id")) is not None:
                span.set_attribute("iot.message_id", message_id)
        else:
            span.set_attribute("iot.outcome", "rejected")
            span.set_attribute("iot.reject_reason", reason)
            span.set_attribute("iot.reject_detail", str(info)[:256])
        carrier: dict[str, str] = {}
        self._propagator.inject(carrier, context=self._set_span_in_context(span))
        return [(k, v.encode()) for k, v in carrier.items()]

    def on_delivery(self, span: Any, ack: Callable[[Any, Any], None]) -> Callable[[Any, Any], None]:
        """Delivery callback that ends the span when Kafka acknowledged (or failed) the record."""
        def done(err: Any, msg: Any) -> None:
            if err is not None:
                span.set_status(self._status_error, str(err))
            span.end()
            ack(err, msg)
        return done

    def shutdown(self) -> None:
        self.provider.shutdown()  # exports pending spans
