import base64

import orjson
import pytest

from telemetry_processor.benchmark import make_detection, make_envelope
from telemetry_processor.normalize import (
    DETECTION,
    ENVELOPE_MAX_CHARS,
    REASONS,
    REJECTED,
    process,
    reject_value,
)

NOW = 1789377528000


def run(env) -> tuple:
    """(reason, key, record, info); reason is None for an accepted record."""
    value = env if isinstance(env, bytes) or env is None else orjson.dumps(env)
    kind, key, record, info = process(value, NOW)
    rec = orjson.loads(record)
    assert kind in (DETECTION, REJECTED)
    return (info if kind is REJECTED else None), key, rec, info


def assert_rejected(env, reason: str, key=None, detail: str | None = None) -> dict:
    got_reason, got_key, rec, info = run(env)
    assert got_reason == reason, rec
    assert got_key == key
    assert rec["reason"] == reason and rec["processed_ts"] == NOW
    assert set(rec) == {"reason", "detail", "topic", "client_cert_cn", "received_ts", "processed_ts", "envelope"}
    if detail is not None:
        assert detail in rec["detail"]
    return rec


# --- happy path ----------------------------------------------------------------

def test_valid_envelope_is_normalized_unchanged_plus_three_fields():
    d = make_detection()
    reason, key, rec, info = run(make_envelope(d))
    assert reason is None and key == "T000042"
    assert rec == d | {"received_ts": 1789377527990, "processed_ts": NOW, "tbmq_node": "tbmq-0"}
    assert list(rec)[:len(d)] == list(d)  # field order kept
    assert info["ts"] == d["ts"]


def test_optional_and_unknown_fields_pass_through():
    d = make_detection(vehicle_id="T000042-V12", true_class="car", extra={"x": 1})
    reason, _, rec, _ = run(make_envelope(d))
    assert reason is None
    assert rec["vehicle_id"] == "T000042-V12" and rec["true_class"] == "car" and rec["extra"] == {"x": 1}


@pytest.mark.parametrize("position", ["start", "middle", "end"])
def test_all_positions_and_integer_numbers_accepted(position):
    d = make_detection(position=position, speed_kmh=66, length_m=16, confidence=1, direction="start_to_end",
                       classification="OTOMOBIL")
    assert run(make_envelope(d))[0] is None


def test_missing_tbmq_node_is_null():
    env = make_envelope()
    del env["tbmqNode"]
    assert run(env)[2]["tbmq_node"] is None


def test_every_reason_is_reachable():
    assert set(REASONS) == {"bad_envelope", "bad_topic", "identity_mismatch", "bad_payload", "payload_mismatch",
                            "unsupported_schema"}


# --- bad_envelope --------------------------------------------------------------

@pytest.mark.parametrize("value, detail", [
    (None, "null"),
    (b"", "not valid UTF-8 JSON"),
    (b"not json", "not valid UTF-8 JSON"),
    (b'{"topicName": "\xff"}', "not valid UTF-8 JSON"),
    (b"[1, 2]", "not a JSON object"),
    (b'"string"', "not a JSON object"),
])
def test_bad_envelope_unparseable(value, detail):
    rec = assert_rejected(value, "bad_envelope", detail=detail)
    assert rec["topic"] is None and rec["client_cert_cn"] is None and rec["received_ts"] is None


@pytest.mark.parametrize("change, detail", [
    ({"topicName": None}, "topicName"),
    ({"topicName": 42}, "topicName"),
    ({"payload": None}, "payload missing"),
    ({"payload": ["eyJ9"]}, "payload missing"),
    ({"ts": None}, "ts"),
    ({"ts": "1789377527990"}, "ts"),
    ({"ts": 1.5}, "ts"),
    ({"ts": True}, "ts"),
])
def test_bad_envelope_fields(change, detail):
    env = make_envelope() | change
    rec = assert_rejected(env, "bad_envelope", detail=detail)
    assert rec["client_cert_cn"] == "T000042"


def test_bad_envelope_missing_keys():
    for key in ("topicName", "payload", "ts"):
        env = make_envelope()
        del env[key]
        assert_rejected(env, "bad_envelope", detail=key if key != "payload" else "payload missing")


@pytest.mark.parametrize("payload", [
    "eyJhIjoxfQ",            # missing padding
    "eyJhIjoxfQ===",         # excess padding
    "eyJh IjoxfQ==",         # whitespace
    "eyJh\nIjoxfQ==",        # newline
    "eyJhIjoxfQ==eyJh",      # data after padding
    "-_-_",                  # URL-safe alphabet
    "ëyJhIjoxfQ==",          # non-ASCII
])
def test_bad_base64(payload):
    rec = assert_rejected(make_envelope(raw_payload=payload), "bad_envelope", detail="base64")
    assert rec["topic"] == "tunnels/T000042/sensors/middle/detections" and rec["received_ts"] == 1789377527990


# --- bad_topic / identity_mismatch -----------------------------------------------

@pytest.mark.parametrize("topic", [
    "tunnels/T000042/sensors/side/detections",
    "tunnels/T000042/sensors/middle/telemetry",
    "tunnels//sensors/middle/detections",
    "tunnel/T000042/sensors/middle/detections",
    "tunnels/T000042/sensor/middle/detections",
    "tunnels/T000042/sensors/middle/detections/x",
    "/tunnels/T000042/sensors/middle/detections",
    "",
])
def test_bad_topic(topic):
    rec = assert_rejected(make_envelope(topic=topic), "bad_topic", key=None)
    assert rec["topic"] == topic


@pytest.mark.parametrize("cn, detail", [(None, "missing"), ("T000043", "'T000043' != topic tunnel_id 'T000042'"),
                                        ("", "!="), (42, "!="), ("t000042", "!=")])
def test_identity_mismatch(cn, detail):
    rec = assert_rejected(make_envelope(cn=cn), "identity_mismatch", key="T000042", detail=detail)
    assert rec["client_cert_cn"] == (cn if isinstance(cn, str) else None)


def test_identity_null_cn():
    env = make_envelope()
    env["clientCertCn"] = None
    assert_rejected(env, "identity_mismatch", key="T000042", detail="missing")


def test_identity_checked_before_payload():
    assert_rejected(make_envelope(b"garbage", cn="T000001"), "identity_mismatch", key="T000042")


# --- bad_payload / unsupported_schema / payload_mismatch -----------------------------

@pytest.mark.parametrize("payload, detail", [
    (b"", "not valid UTF-8 JSON"),
    (b"not json", "not valid UTF-8 JSON"),
    (b'{"schema": 1, "message_id": "\xff"}', "not valid UTF-8 JSON"),
    (b'{"schema": 1, "message_id": "\\ud800"}', "not valid UTF-8 JSON"),   # lone surrogate
    (b"\xef\xbb\xbf{}", "not valid UTF-8 JSON"),                         # BOM
    (b"[1]", "not a JSON object"),
    (b"null", "not a JSON object"),
    (b"{}", "missing field 'schema'"),
])
def test_bad_payload_unparseable(payload, detail):
    assert_rejected(make_envelope(payload), "bad_payload", key="T000042", detail=detail)


@pytest.mark.parametrize("schema", [1, 3, 0, "2", 2.0, None])
def test_unsupported_schema(schema):
    assert_rejected(make_envelope(make_detection(schema=schema)), "unsupported_schema", key="T000042")


FIELDS = ("message_id", "sensor_id", "tunnel_id", "position", "seq", "ts", "direction", "lane", "speed_kmh",
          "length_m", "occupancy_ms", "classification", "confidence")


@pytest.mark.parametrize("field", FIELDS)
def test_missing_required_field(field):
    d = make_detection()
    del d[field]
    assert_rejected(make_envelope(d), "bad_payload", key="T000042", detail=f"missing field '{field}'")


@pytest.mark.parametrize("field, value", [
    ("message_id", 1), ("sensor_id", None), ("tunnel_id", ["T000042"]), ("position", 2),
    ("seq", "1"), ("seq", 1.0), ("seq", True), ("ts", 1.7e12), ("lane", "1"), ("occupancy_ms", 871.5),
    ("speed_kmh", "66.2"), ("speed_kmh", True), ("length_m", None), ("confidence", "0.9"),
    ("direction", "left"), ("direction", ["start_to_end"]), ("classification", "bus"), ("classification", {"a": 1}),
])
def test_mistyped_field(field, value):
    env = make_envelope(make_detection(**{field: value}), tunnel_id="T000042", position="middle")
    rec = assert_rejected(env, "bad_payload", key="T000042")
    assert f"'{field}'" in rec["detail"]


def test_integer_beyond_64_bits_is_mistyped():
    payload = orjson.dumps(make_detection()).replace(b'"seq":1234', b'"seq":123456789012345678901234567890')
    rec = assert_rejected(make_envelope(payload), "bad_payload", key="T000042")
    assert "'seq'" in rec["detail"]


@pytest.mark.parametrize("change", [{"tunnel_id": "T000043"}, {"position": "start"},
                                    {"sensor_id": "T000042-start"}, {"sensor_id": "T000042middle"}])
def test_payload_mismatch(change):
    env = make_envelope(make_detection() | change, tunnel_id="T000042", position="middle")
    assert_rejected(env, "payload_mismatch", key="T000042")


def test_payload_mismatch_wrong_topic_position():
    env = make_envelope(make_detection(position="middle"), position="end")
    assert_rejected(env, "payload_mismatch", key="T000042", detail="'middle'")


# --- rejection record ------------------------------------------------------------------

def test_rejection_envelope_is_original_text_truncated():
    env = make_envelope(cn="T1", filler="é" * 5000)
    value = orjson.dumps(env)
    rec = assert_rejected(value, "identity_mismatch", key="T000042")
    assert len(rec["envelope"]) == ENVELOPE_MAX_CHARS
    assert value.decode().startswith(rec["envelope"])


def test_rejection_envelope_invalid_utf8_replaced():
    rec = assert_rejected(b'{"topicName": "\xff\xfe"}', "bad_envelope")
    assert rec["envelope"] == '{"topicName": "��"}'


def test_rejection_short_envelope_kept_whole():
    env = make_envelope(make_detection(schema=7))
    value = orjson.dumps(env)
    rec = assert_rejected(value, "unsupported_schema", key="T000042")
    assert rec["envelope"] == value.decode()
    assert rec["topic"] == env["topicName"] and rec["client_cert_cn"] == "T000042"
    assert rec["received_ts"] == 1789377527990


def test_reject_value_after_validation():
    value = orjson.dumps(make_envelope())
    kind, key, record, reason = reject_value(value, NOW, "bad_payload", "producer refused")
    rec = orjson.loads(record)
    assert kind is REJECTED
    assert (reason, key, rec["detail"], rec["client_cert_cn"]) == ("bad_payload", "T000042", "producer refused",
                                                                     "T000042")


def test_base64_roundtrip_binary_payload_is_bad_payload():
    env = make_envelope(bytes(range(256)))
    assert base64.b64decode(env["payload"]) == bytes(range(256))
    assert_rejected(env, "bad_payload", key="T000042")
