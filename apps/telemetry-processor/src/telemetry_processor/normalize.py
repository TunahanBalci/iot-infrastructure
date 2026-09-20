"""Envelope validation and normalization. Pure functions, no Kafka: the hot path of the service.

Input  (iot.mqtt.ingest):          TBMQ integration-executor envelope, MQTT payload base64-encoded.
Output (iot.detections):           the simulator detection, unchanged, plus received_ts, processed_ts, tbmq_node.
Output (iot.sensor-health):        the device's own health report, plus the same three fields.
Output (iot.detections.rejected):  reason, detail, topic, client_cert_cn, received_ts, processed_ts, envelope.

Devices publish on two topics, which is what `kind` distinguishes:
    tunnels/{tunnel_id}/sensors/{position}/detections
    tunnels/{tunnel_id}/sensors/{position}/health
"""

from __future__ import annotations

import binascii
from typing import Any

import orjson

SCHEMA_VERSION = 2
POSITIONS = frozenset(("start", "middle", "end"))
DIRECTIONS = frozenset(("start_to_end", "end_to_start"))
CLASSIFICATIONS = frozenset((
    "MOTOSIKLET", "OTOMOBIL", "HAFIF_TICARI", "MINIBUS", "OTOBUS", "KAMYON",
    "CEKICI_YARI_ROMORK", "TRAKTOR", "OZEL_AMACLI_TASIT", "BILINMEYEN",
))
STATUSES = frozenset(("ok", "degraded", "silent"))

# What a record is, and therefore which topic it goes to.
DETECTION = "detection"
HEALTH = "health"
REJECTED = "rejected"

# Checks run envelope -> topic -> identity -> payload JSON -> schema -> fields -> payload vs topic;
# the first failing check decides the reason.
REASONS = ("bad_envelope", "bad_topic", "identity_mismatch", "bad_payload", "payload_mismatch", "unsupported_schema")
ENVELOPE_MAX_CHARS = 4096

STR_FIELDS = ("message_id", "sensor_id", "device_id", "device_serial", "tunnel_id", "position", "plate")
INT_FIELDS = ("seq", "ts", "lane", "occupancy_ms")
NUMBER_FIELDS = ("speed_kmh", "length_m", "confidence")
ENUM_FIELDS = (("direction", DIRECTIONS), ("classification", CLASSIFICATIONS))

HEALTH_STR_FIELDS = ("sensor_id", "device_id", "device_serial", "tunnel_id", "position")
HEALTH_INT_FIELDS = ("ts", "detections", "duplicates", "missed")
HEALTH_NUMBER_FIELDS = ("window_s", "uptime_s")
HEALTH_ENUM_FIELDS = (("status", STATUSES),)

_loads = orjson.loads
_dumps = orjson.dumps
_JSONDecodeError = orjson.JSONDecodeError
_a2b_base64 = binascii.a2b_base64
_MISSING = object()

# (kind, tunnel_id, record, info)
#   detection: (DETECTION, tunnel_id,         detection JSON, payload dict)
#   health:    (HEALTH,    tunnel_id,         health JSON,    payload dict)
#   rejected:  (REJECTED,  tunnel_id or None, rejection JSON, reason str)
Result = tuple[str, str | None, bytes, Any]


def process(value: bytes | None, now_ms: int) -> Result:
    """Validate one iot.mqtt.ingest record value and build the record to produce."""
    try:
        env = _loads(value)
    except (_JSONDecodeError, TypeError):
        return reject(value, now_ms, None, None, "bad_envelope",
                      "record value is null" if value is None else "envelope is not valid UTF-8 JSON")
    if type(env) is not dict:
        return reject(value, now_ms, None, None, "bad_envelope", "envelope is not a JSON object")

    topic = env.get("topicName")
    if type(topic) is not str:
        return reject(value, now_ms, env, None, "bad_envelope", "topicName missing or not a string")
    b64 = env.get("payload")
    if type(b64) is not str:
        return reject(value, now_ms, env, None, "bad_envelope", "payload missing or not a string")
    try:
        raw = _a2b_base64(b64, strict_mode=True)
    except ValueError:  # binascii.Error, or non-ASCII characters
        return reject(value, now_ms, env, None, "bad_envelope", "payload is not valid base64")
    received_ts = env.get("ts")
    if type(received_ts) is not int:
        return reject(value, now_ms, env, None, "bad_envelope", "ts missing or not an integer")

    parts = topic.split("/")
    if (len(parts) != 5 or parts[0] != "tunnels" or parts[2] != "sensors" or not parts[1]
            or parts[3] not in POSITIONS or parts[4] not in ("detections", "health")):
        return reject(value, now_ms, env, None, "bad_topic",
                      "topic is not tunnels/{tunnel_id}/sensors/{start|middle|end}/{detections|health}")
    tunnel_id = parts[1]
    position = parts[3]
    is_health = parts[4] == "health"

    # Identity: the certificate CN is the tunnel id. The MQTT ACL already restricts what a
    # client may publish; this is defense in depth against ACL mistakes.
    cn = env.get("clientCertCn")
    if cn != tunnel_id:
        return reject(value, now_ms, env, tunnel_id, "identity_mismatch",
                      "clientCertCn missing" if cn is None else f"clientCertCn {cn!r} != topic tunnel_id {tunnel_id!r}")

    try:
        d = _loads(raw)
    except _JSONDecodeError:
        return reject(value, now_ms, env, tunnel_id, "bad_payload", "payload is not valid UTF-8 JSON")
    if type(d) is not dict:
        return reject(value, now_ms, env, tunnel_id, "bad_payload", "payload is not a JSON object")
    schema = d.get("schema", _MISSING)
    if type(schema) is not int or schema != SCHEMA_VERSION:
        if schema is _MISSING:
            return reject(value, now_ms, env, tunnel_id, "bad_payload", "missing field 'schema'")
        return reject(value, now_ms, env, tunnel_id, "unsupported_schema", f"schema {schema!r} (supported: 2)")

    g = d.get
    if is_health:
        status = g("status")
        if not (type(g("sensor_id")) is str and type(g("device_id")) is str and type(g("device_serial")) is str
                and type(g("tunnel_id")) is str and type(g("position")) is str and type(g("ts")) is int
                and type(g("detections")) is int and type(g("duplicates")) is int and type(g("missed")) is int
                and type(g("degraded")) is bool
                and type(g("window_s")) in (int, float) and type(g("uptime_s")) in (int, float)
                and type(status) is str and status in STATUSES):
            return reject(value, now_ms, env, tunnel_id, "bad_payload", _health_error(d))
    else:
        # Fast path: one expression; the slow path below only runs to explain a failure.
        speed, length, confidence = g("speed_kmh"), g("length_m"), g("confidence")
        direction, classification = g("direction"), g("classification")
        if not (type(g("message_id")) is str and type(g("sensor_id")) is str and type(g("device_id")) is str
                and type(g("device_serial")) is str and type(g("tunnel_id")) is str and type(g("plate")) is str
                and type(g("position")) is str and type(g("seq")) is int and type(g("ts")) is int
                and type(g("lane")) is int and type(g("occupancy_ms")) is int
                and (type(speed) is float or type(speed) is int) and (type(length) is float or type(length) is int)
                and (type(confidence) is float or type(confidence) is int)
                and type(direction) is str and direction in DIRECTIONS
                and type(classification) is str and classification in CLASSIFICATIONS):
            return reject(value, now_ms, env, tunnel_id, "bad_payload", _field_error(d))

    sensor_id = f"{tunnel_id}-{position}"
    if d["tunnel_id"] != tunnel_id or d["position"] != position or d["sensor_id"] != sensor_id:
        return reject(value, now_ms, env, tunnel_id, "payload_mismatch",
                      f"payload tunnel_id/position/sensor_id {d['tunnel_id']!r}/{d['position']!r}/{d['sensor_id']!r} "
                      f"!= topic {tunnel_id!r}/{position!r}/{sensor_id!r}")

    d["received_ts"] = received_ts
    d["processed_ts"] = now_ms
    node = env.get("tbmqNode")
    d["tbmq_node"] = node if type(node) is str else None
    return (HEALTH if is_health else DETECTION), tunnel_id, _dumps(d), d


def _missing_or_wrong(d: dict, str_fields, int_fields, number_fields, enum_fields) -> str:
    for name in str_fields + int_fields + number_fields + tuple(n for n, _ in enum_fields):
        if name not in d:
            return f"missing field {name!r}"
    for name in str_fields:
        if type(d[name]) is not str:
            return f"field {name!r} must be a string"
    for name in int_fields:
        if type(d[name]) is not int:
            return f"field {name!r} must be an integer"
    for name in number_fields:
        if type(d[name]) not in (int, float):
            return f"field {name!r} must be a number"
    for name, allowed in enum_fields:
        v = d[name]
        if type(v) is not str or v not in allowed:
            return f"field {name!r} must be one of {'|'.join(sorted(allowed))}"
    return ""


def _field_error(d: dict) -> str:
    return _missing_or_wrong(d, STR_FIELDS, INT_FIELDS, NUMBER_FIELDS, ENUM_FIELDS) or "invalid payload"


def _health_error(d: dict) -> str:
    error = _missing_or_wrong(d, HEALTH_STR_FIELDS, HEALTH_INT_FIELDS, HEALTH_NUMBER_FIELDS, HEALTH_ENUM_FIELDS)
    if error:
        return error
    if "degraded" not in d:
        return "missing field 'degraded'"
    if type(d["degraded"]) is not bool:
        return "field 'degraded' must be a boolean"
    return "invalid health payload"


def reject(value: bytes | None, now_ms: int, env: dict | None, tunnel_id: str | None, reason: str,
           detail: str) -> Result:
    """Rejection record. `env` (the parsed envelope, if any) fills topic, client_cert_cn and received_ts."""
    topic = cn = received_ts = None
    if env is not None:
        topic, cn, received_ts = env.get("topicName"), env.get("clientCertCn"), env.get("ts")
        topic = topic if type(topic) is str else None
        cn = cn if type(cn) is str else None
        received_ts = received_ts if type(received_ts) is int else None
    if value is None:
        text = ""
    else:
        # At most 4 bytes per character: decode only what can survive the truncation.
        text = bytes(value[:4 * ENVELOPE_MAX_CHARS]).decode("utf-8", "replace")[:ENVELOPE_MAX_CHARS]
    record = _dumps({"reason": reason, "detail": detail, "topic": topic, "client_cert_cn": cn,
                     "received_ts": received_ts, "processed_ts": now_ms, "envelope": text})
    return REJECTED, tunnel_id, record, reason


def reject_value(value: bytes | None, now_ms: int, reason: str, detail: str) -> Result:
    """Rejection for a record that failed after validation (e.g. refused by the producer)."""
    try:
        env = _loads(value)
    except (_JSONDecodeError, TypeError):
        env = None
    if type(env) is not dict:
        env = None
    topic = env.get("topicName") if env else None
    parts = topic.split("/") if type(topic) is str else ()
    return reject(value, now_ms, env, parts[1] if len(parts) == 5 and parts[1] else None, reason, detail)
