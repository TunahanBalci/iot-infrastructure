"""Multi-class fusion, plates on events, and the traffic window after the health move."""

import orjson
import pytest

from tunnel_consensus.config import Config
from tunnel_consensus.engine import ConsensusEngine
from tunnel_consensus.model import UNKNOWN, VEHICLE_TYPES

TS0 = 1_800_000_000_000
HALF = 500.0        # sensor spacing; a 72 km/h vehicle takes 25 s between sensors
STEP_MS = 25_000


def detection(position="start", seq=1, ts=TS0, classification="OTOMOBIL", confidence=0.95, **kw) -> bytes:
    sensor_id = f"T000001-{position}"
    body = {"schema": 2, "message_id": f"{sensor_id}:boot:{seq}", "sensor_id": sensor_id,
            "device_id": "02:aa:bb:cc:dd:ee", "device_serial": "TSN-QUT73ELQA5",
            "tunnel_id": "T000001", "position": position, "seq": seq, "ts": ts,
            "direction": "start_to_end", "lane": 1, "plate": "34 ABC 123", "speed_kmh": 72.0,
            "length_m": 4.5, "occupancy_ms": 225, "classification": classification, "confidence": confidence}
    body.update(kw)
    return orjson.dumps(body)


def engine_and_output(cfg: Config | None = None):
    out = []
    engine = ConsensusEngine(cfg or Config(), lambda topic, payload, retain: out.append((topic, orjson.loads(payload))))
    engine.known_halves["T000001"] = HALF
    return engine, out


def fuse(reports, confidences=(0.95, 0.95, 0.95), plates=("34 ABC 123",) * 3) -> dict:
    """One vehicle seen by the three sensors, each reporting its own type."""
    engine, out = engine_and_output()
    for i, (position, vtype, confidence, plate) in enumerate(
            zip(("start", "middle", "end"), reports, confidences, plates)):
        engine.ingest(detection(position=position, seq=i + 1, ts=TS0 + i * STEP_MS,
                                classification=vtype, confidence=confidence, plate=plate))
    engine.flush()
    events = [body for topic, body in out if "vehicles" in topic]
    assert len(events) == 1, out
    return events[0]


# --- fusion --------------------------------------------------------------

def test_the_type_the_sensors_agree_on_wins():
    event = fuse(("MINIBUS", "MINIBUS", "MINIBUS"))
    assert event["classification"] == "MINIBUS"
    assert event["agreement"] == 1.0
    assert event["sensors"] == 3


def test_one_disagreeing_sensor_is_outvoted():
    event = fuse(("KAMYON", "OTOBUS", "KAMYON"))
    assert event["classification"] == "KAMYON"
    assert event["agreement"] == pytest.approx(2 / 3, abs=1e-3)


def test_a_confident_sensor_outweighs_two_unsure_ones():
    event = fuse(("OTOMOBIL", "MINIBUS", "OTOMOBIL"), confidences=(0.52, 0.99, 0.52))
    assert event["classification"] == "MINIBUS"


def test_unknown_readings_abstain_instead_of_winning():
    event = fuse((UNKNOWN, "TRAKTOR", UNKNOWN))
    assert event["classification"] == "TRAKTOR"
    assert event["agreement"] == pytest.approx(1 / 3, abs=1e-3)


def test_a_vehicle_no_sensor_could_classify_stays_unknown():
    assert fuse((UNKNOWN, UNKNOWN, UNKNOWN))["classification"] == UNKNOWN


def test_every_turkish_type_survives_fusion():
    for vtype in VEHICLE_TYPES:
        assert fuse((vtype, vtype, vtype))["classification"] == vtype


def test_confidence_rises_with_agreement():
    unanimous = fuse(("KAMYON", "KAMYON", "KAMYON"))["confidence"]
    split = fuse(("KAMYON", "OTOBUS", "KAMYON"))["confidence"]
    assert unanimous > split


# --- plate ---------------------------------------------------------------

def test_the_event_carries_the_plate_of_the_vehicle():
    assert fuse(("OTOMOBIL",) * 3)["plate"] == "34 ABC 123"


def test_detections_that_read_different_plates_are_different_vehicles():
    """Even with timings that would fit, a different plate means a different vehicle."""
    engine, out = engine_and_output()
    for step, (position, plate) in enumerate(zip(("start", "middle", "end"),
                                                 ("34 ABC 123", "34 ABC 124", "34 ABC 123"))):
        engine.ingest(detection(position=position, seq=step + 1, ts=TS0 + step * STEP_MS, plate=plate))
    engine.flush()
    events = [body for topic, body in out if "vehicles" in topic]
    assert len(events) == 2
    assert sorted(e["plate"] for e in events) == ["34 ABC 123", "34 ABC 124"]


# --- reporting -----------------------------------------------------------

def test_the_traffic_window_counts_vehicles_per_type_and_direction():
    engine, out = engine_and_output()
    for i, vtype in enumerate(("OTOMOBIL", "OTOMOBIL", "KAMYON")):
        base = TS0 + i * 200_000
        for step, position in enumerate(("start", "middle", "end")):
            engine.ingest(detection(position=position, seq=i * 3 + step + 1,
                                    ts=base + step * STEP_MS, classification=vtype))
    engine.flush()
    engine.report_traffic(window_s=60.0, now_ms=TS0 + 600_000)
    traffic = [body for topic, body in out if "traffic" in topic]
    assert traffic, out
    counts = traffic[-1]["counts"]["start_to_end"]
    assert counts == {"OTOMOBIL": 2, "KAMYON": 1}
    assert traffic[-1]["vehicles"] == 3


def test_consensus_no_longer_reports_sensor_health():
    """The devices report their own health now; nothing here publishes it."""
    engine, out = engine_and_output()
    assert not hasattr(engine, "report_health")
    for step, position in enumerate(("start", "middle", "end")):
        engine.ingest(detection(position=position, seq=step + 1, ts=TS0 + step * STEP_MS))
    engine.flush()
    engine.report_traffic(window_s=60.0, now_ms=TS0 + 60_000)
    assert not [topic for topic, _ in out if topic.endswith("/health")]


def test_health_topics_are_gone_from_the_configuration():
    cfg = Config()
    assert not hasattr(cfg.output, "sensor_health_topic_template")
    assert not hasattr(cfg.kafka, "sensor_health_topic")


# --- association ---------------------------------------------------------

def test_two_vehicles_too_close_to_tell_apart_are_split_by_their_plates():
    """Same lane, 1.2 s apart, same speed: only the plate says which detection belongs to which."""
    engine, out = engine_and_output()
    for i, plate in enumerate(("34 AAA 111", "34 BBB 222")):
        for step, position in enumerate(("start", "middle", "end")):
            engine.ingest(detection(position=position, seq=i * 3 + step + 1,
                                    ts=TS0 + i * 1_200 + step * STEP_MS, plate=plate))
    engine.flush()
    events = [body for topic, body in out if "vehicles" in topic]
    assert len(events) == 2
    assert {e["plate"] for e in events} == {"34 AAA 111", "34 BBB 222"}
    assert all(e["sensors"] == 3 for e in events)


def test_association_still_works_when_the_plate_is_unreadable():
    """A device that could not read the plate falls back to timing, as before."""
    engine, out = engine_and_output()
    for step, position in enumerate(("start", "middle", "end")):
        engine.ingest(detection(position=position, seq=step + 1, ts=TS0 + step * STEP_MS,
                                plate="" if position == "middle" else "34 ABC 123"))
    engine.flush()
    events = [body for topic, body in out if "vehicles" in topic]
    assert len(events) == 1 and events[0]["sensors"] == 3
    assert events[0]["plate"] == "34 ABC 123"
