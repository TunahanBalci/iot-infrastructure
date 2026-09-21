from collections import Counter

import orjson
import pytest

from tunnel_consensus.config import Config, apply_env_overrides, load_config
from tunnel_consensus.engine import ConsensusEngine
from tunnel_consensus.model import InvalidDetection, SensorState, parse_detection
from tunnel_consensus.sharding import Assignment, partition_of


def detection(seq=1, position="start", ts=1_800_000_000_000, direction="start_to_end", **kw) -> bytes:
    body = {"schema": 2, "message_id": f"T000001-{position}:boot:{seq}", "sensor_id": f"T000001-{position}",
            "device_id": "02:aa:bb:cc:dd:ee", "device_serial": "TSN-QUT73ELQA5",
            "tunnel_id": "T000001", "position": position, "seq": seq, "ts": ts, "direction": direction,
            "lane": 1, "plate": "34 ABC 123", "speed_kmh": 72.0, "length_m": 4.5, "occupancy_ms": 225,
            "classification": "OTOMOBIL", "confidence": 0.95}
    body.update(kw)
    return orjson.dumps(body)


def capture_engine(cfg: Config | None = None, half: float = 500.0):
    out = []
    engine = ConsensusEngine(cfg or Config(), lambda topic, payload, retain: out.append((topic, orjson.loads(payload))))
    engine.known_halves["T000001"] = half   # skip calibration
    return engine, out


# --- config ----------------------------------------------------------------

def test_default_config_file_matches_model_defaults():
    assert load_config("config/consensus.yaml", environ={}) == Config()


def test_env_override_keeps_numeric_password_a_string():
    data = apply_env_overrides({}, {"CONSENSUS__MQTT__PASSWORD": "123456", "CONSENSUS__PARTITIONING__REPLICAS": "3"})
    cfg = Config.model_validate(data)
    assert cfg.mqtt.password == "123456"
    assert cfg.partitioning.replicas == 3


def test_client_id_prefix_must_be_alphanumeric():
    with pytest.raises(ValueError):
        Config.model_validate({"mqtt": {"client_id_prefix": "consensus-1"}})


def test_ordinal_from_statefulset_hostname():
    p = Config.model_validate({"partitioning": {"replicas": 3}}).partitioning
    assert p.resolved_ordinal("consensus-2") == 2
    assert p.resolved_ordinal("laptop") == 0
    with pytest.raises(ValueError):
        p.resolved_ordinal("consensus-3")


# --- parsing / dedup ---------------------------------------------------------

def test_parse_rejects_bad_messages():
    d = parse_detection(detection(direction="end_to_start"))
    assert (d.position, d.index, d.boot_id, d.speed_ms) == ("start", 2, "boot", 20.0)
    for bad in (b"not json", detection(position="side"), detection(classification="bus"),
                detection(schema=1), detection(speed_kmh=0), orjson.dumps({"schema": 2})):
        with pytest.raises(InvalidDetection):
            parse_detection(bad)


def test_sensor_dedup_and_loss_accounting():
    s = SensorState("T1-start", "start", window=8, speed_noise=0.03)
    assert s.accept("a", 1)
    assert not s.accept("a", 1)          # duplicate
    assert s.accept("a", 4)              # 2 and 3 lost
    assert s.w_lost == 2
    assert s.accept("a", 3)              # arrived late after all
    assert s.w_lost == 1 and s.lost_total == 1
    assert not s.accept("a", 3)
    assert s.accept("b", 1)              # sensor restarted: new boot id
    assert s.w_duplicates == 2


# --- sharding ----------------------------------------------------------------

def test_every_tunnel_has_exactly_one_partition():
    cfg = Config.model_validate({"partitioning": {"replicas": 2, "workers": 3, "tunnels": 3000}})
    owned = Counter()
    for ordinal in range(2):
        for w in range(3):
            a = Assignment.for_worker(cfg, ordinal, w)
            ids = a.tunnel_ids()
            owned.update(ids)
            assert all(partition_of(t, 6) == a.partition for t in ids)
            assert a.client_id().isalnum()
            assert a.topic_filters()[0] == f"tunnels/{ids[0]}/sensors/+/detections"
            assert len(ids) > 400  # roughly balanced
    assert len(owned) == 3000 and set(owned.values()) == {1}


def test_unsharded_uses_one_wildcard():
    a = Assignment.for_worker(Config(), 0, 0)
    assert a.topic_filters() == ["tunnels/+/sensors/+/detections"]
    assert a.owns("anything")


# --- engine ------------------------------------------------------------------

def test_invalid_and_foreign_detections_are_counted():
    engine = ConsensusEngine(Config(), lambda *a: None, owns=lambda tid: tid != "T000001")
    engine.ingest(b"{")
    engine.ingest(detection())
    assert engine.stats.invalid == 1 and engine.stats.foreign == 1 and not engine.tunnels


def test_three_sensors_fuse_into_one_event():
    engine, out = capture_engine()
    t0 = 1_800_000_000_000
    # 72 km/h = 20 m/s; sensors 500 m apart -> 25 s per hop. The middle sensor misclassifies.
    engine.ingest(detection(1, "start", t0))
    engine.ingest(detection(1, "middle", t0 + 25_010, classification="KAMYON", confidence=0.6))
    engine.ingest(detection(1, "end", t0 + 50_000))
    assert len(out) == 1
    topic, v = out[0]
    assert topic == "tunnels/T000001/vehicles"
    assert (v["sensors"], v["classification"], v["missing"]) == (3, "OTOMOBIL", [])
    assert v["agreement"] == pytest.approx(2 / 3, abs=1e-3)
    assert v["speed_kmh"] == pytest.approx(72.0)
    assert (v["ts_entry"], v["ts_exit"]) == (t0, t0 + 50_000)
    assert engine.stats.pending == 0


def test_missed_last_sensor_emits_pair_after_deadline():
    engine, out = capture_engine()
    t0 = 1_800_000_000_000
    engine.ingest(detection(1, "start", t0))
    engine.ingest(detection(1, "middle", t0 + 25_000))
    assert out == []
    engine.ingest(detection(2, "start", t0 + 80_000, lane=2))  # later traffic advances the watermark
    assert len(out) == 1
    v = out[0][1]
    assert v["sensors"] == 2 and v["missing"] == ["end"]
    assert v["ts_exit"] == t0 + 50_000


def test_reverse_direction_and_lanes_are_separate():
    engine, out = capture_engine()
    t0 = 1_800_000_000_000
    engine.ingest(detection(1, "end", t0, direction="end_to_start"))
    engine.ingest(detection(1, "start", t0 + 1_000, lane=2))
    engine.ingest(detection(1, "middle", t0 + 25_000, direction="end_to_start"))
    engine.ingest(detection(2, "middle", t0 + 26_000, lane=2))
    engine.ingest(detection(2, "start", t0 + 50_000, direction="end_to_start"))
    engine.ingest(detection(2, "end", t0 + 51_000, lane=2))
    assert [(v["direction"], v["lane"], v["sensors"]) for _, v in out] == [
        ("end_to_start", 1, 3), ("start_to_end", 2, 3)]


def test_idle_tick_flushes_last_vehicles():
    engine, out = capture_engine()
    engine.ingest(detection(1, "start", 1_800_000_000_000), wall_now=100.0)
    engine.tick(110.0)
    assert out == []
    engine.tick(160.0)  # 60 s idle: vehicle should have left long ago
    assert len(out) == 1 and out[0][1]["sensors"] == 1


def test_flush_emits_everything_pending():
    engine, out = capture_engine()
    engine.ingest(detection(1, "start", 1_800_000_000_000))
    engine.flush()
    assert len(out) == 1 and engine.stats.pending == 0


def test_geometry_bounds_cover_the_deployed_tunnels():
    """A tunnel longer than length_m[1] can never calibrate, so it emits no vehicles at all."""
    import yaml
    from tunnel_consensus.geometry import GeometryParams

    p = GeometryParams(Config().geometry)
    profiles = yaml.safe_load(open("../simulator/config/tunnels.yaml"))["tunnels"]
    longest = max(t["length_m"] for t in profiles)
    assert p.min_half <= longest / 2 <= p.max_half, (
        f"longest profile tunnel is {longest} m (half {longest / 2}), "
        f"calibrator only accepts halves in [{p.min_half}, {p.max_half}]")
