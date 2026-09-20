"""Schema 2 detections (device identity, plate, Turkish types) and the device health route."""

import orjson
import pytest

from telemetry_processor.benchmark import make_detection, make_envelope, make_health
from telemetry_processor.normalize import DETECTION, HEALTH, REJECTED, process

NOW = 1789377528000


def run(env) -> tuple:
    value = env if isinstance(env, bytes) or env is None else orjson.dumps(env)
    kind, key, record, info = process(value, NOW)
    return kind, key, orjson.loads(record), info


# --- schema 2 detections -------------------------------------------------

def test_a_schema_2_detection_is_accepted_with_its_new_fields():
    d = make_detection()
    kind, key, rec, _ = run(make_envelope(d))
    assert kind == DETECTION and key == "T000042"
    assert rec["schema"] == 2
    assert rec["device_id"] == d["device_id"] and rec["device_serial"] == d["device_serial"]
    assert rec["plate"] == d["plate"] and rec["classification"] == d["classification"]
    assert rec["received_ts"] == 1789377527990 and rec["processed_ts"] == NOW


def test_schema_1_is_no_longer_supported():
    kind, _, rec, _ = run(make_envelope(make_detection(schema=1)))
    assert kind == REJECTED and rec["reason"] == "unsupported_schema"


@pytest.mark.parametrize("vehicle_type", [
    "MOTOSIKLET", "OTOMOBIL", "HAFIF_TICARI", "MINIBUS", "OTOBUS", "KAMYON",
    "CEKICI_YARI_ROMORK", "TRAKTOR", "OZEL_AMACLI_TASIT", "BILINMEYEN",
])
def test_every_turkish_type_is_a_valid_classification(vehicle_type):
    assert run(make_envelope(make_detection(classification=vehicle_type)))[0] == DETECTION


def test_the_old_english_classes_are_rejected():
    kind, _, rec, _ = run(make_envelope(make_detection(classification="truck")))
    assert kind == REJECTED and rec["reason"] == "bad_payload"
    assert "classification" in rec["detail"]


@pytest.mark.parametrize("field", ["device_id", "device_serial", "plate"])
def test_a_detection_without_its_device_identity_is_rejected(field):
    d = make_detection()
    del d[field]
    kind, _, rec, _ = run(make_envelope(d))
    assert kind == REJECTED and rec["reason"] == "bad_payload"
    assert field in rec["detail"]


def test_device_fields_must_be_strings():
    kind, _, rec, _ = run(make_envelope(make_detection(device_id=1234)))
    assert kind == REJECTED and rec["reason"] == "bad_payload"


# --- health route --------------------------------------------------------

def test_a_health_message_is_routed_to_the_health_output():
    h = make_health()
    kind, key, rec, _ = run(make_envelope(h, topic="tunnels/T000042/sensors/middle/health"))
    assert kind == HEALTH and key == "T000042"
    assert rec["sensor_id"] == h["sensor_id"] and rec["status"] == h["status"]
    assert rec["device_id"] == h["device_id"] and rec["detections"] == h["detections"]
    assert rec["received_ts"] == 1789377527990 and rec["processed_ts"] == NOW


def test_a_detection_on_the_health_topic_is_rejected():
    kind, _, rec, _ = run(make_envelope(make_detection(), topic="tunnels/T000042/sensors/middle/health"))
    assert kind == REJECTED and rec["reason"] == "bad_payload"


def test_a_health_message_on_the_detection_topic_is_rejected():
    kind, _, rec, _ = run(make_envelope(make_health()))
    assert kind == REJECTED and rec["reason"] == "bad_payload"


def test_health_identity_is_checked_like_a_detection():
    kind, _, rec, _ = run(make_envelope(make_health(), topic="tunnels/T000042/sensors/middle/health",
                                        cn="T000099"))
    assert kind == REJECTED and rec["reason"] == "identity_mismatch"


def test_health_must_match_its_topic():
    h = make_health(position="start")
    kind, _, rec, _ = run(make_envelope(h, topic="tunnels/T000042/sensors/middle/health"))
    assert kind == REJECTED and rec["reason"] == "payload_mismatch"


def test_an_unknown_status_is_rejected():
    kind, _, rec, _ = run(make_envelope(make_health(status="fine"),
                                        topic="tunnels/T000042/sensors/middle/health"))
    assert kind == REJECTED and rec["reason"] == "bad_payload"


def test_an_unknown_topic_suffix_is_still_a_bad_topic():
    kind, _, rec, _ = run(make_envelope(make_detection(), topic="tunnels/T000042/sensors/middle/telemetry"))
    assert kind == REJECTED and rec["reason"] == "bad_topic"
