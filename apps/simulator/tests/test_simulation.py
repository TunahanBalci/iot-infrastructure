import heapq
import random
from collections import defaultdict

import orjson
import pytest

from tunnel_sim.config import Config, apply_env_overrides, load_config
from tunnel_sim.model import END_TO_START, START_TO_END, VehicleFactory, build_tunnel, detect
from tunnel_sim.vehicles import VEHICLE_TYPES
from tunnel_sim.sinks import Sink
from tunnel_sim.worker import Worker


def noiseless_config(**sensor_overrides) -> Config:
    sensors = dict(length_noise_std_m=0, misclassification_rate=0, miss_rate=0, duplicate_rate=0,
                   timestamp_jitter_ms=0, speed_noise_ratio=0, degraded_ratio=0)
    sensors.update(sensor_overrides)
    return Config.model_validate({"topology": {"tunnels": 20}, "sensors": sensors})


class CaptureSink(Sink):
    def __init__(self):
        self.messages = []

    def publish(self, sensor, payload):
        self.messages.append((sensor.topic, orjson.loads(payload)))
        return True


def run_sim(cfg: Config, seconds: float) -> CaptureSink:
    """Drive a worker in simulated (not wall-clock) time."""
    sink = CaptureSink()
    w = Worker(cfg, worker_id=0, n_workers=1, boot_id="test", sink=sink)
    w.seed_arrivals()
    w.fast_forward()
    while w.heap and w.heap[0][0] <= seconds:
        w._handle(heapq.heappop(w.heap), live=True)
    return sink


# --- physics -------------------------------------------------------------

@pytest.mark.parametrize("direction", [START_TO_END, END_TO_START])
def test_sensor_timing_follows_speed_and_direction(direction):
    cfg = noiseless_config()
    tunnel = build_tunnel(cfg, 0)
    rng = random.Random(1)
    v = VehicleFactory(cfg, rng).spawn(tunnel, t_entry=100.0)
    v.direction = direction
    v.order = tunnel.sensors_in_travel_order(direction)

    dets = [detect(s, v, rng) for s in v.order]
    times = [d.t_reported for d in dets]
    assert times == sorted(times)
    expected_first = "start" if direction == START_TO_END else "end"
    assert dets[0].sensor.position == expected_first
    assert dets[1].sensor.position == "middle"
    half = tunnel.length_m / 2
    assert times[0] == pytest.approx(100.0)
    assert times[1] - times[0] == pytest.approx(half / v.speed_ms)
    assert times[2] - times[1] == pytest.approx(half / v.speed_ms)
    for d in dets:
        assert d.speed_kmh == pytest.approx(v.speed_ms * 3.6)
        assert d.length_m == pytest.approx(v.length_m)
        assert d.classification == v.vtype


def test_measured_speed_consistent_with_sensor_spacing_under_noise():
    cfg = Config.model_validate({"topology": {"tunnels": 5}, "payload": {"include_vehicle_id": True}})
    sink = run_sim(cfg, 600)
    by_vehicle = defaultdict(dict)
    for _, m in sink.messages:
        by_vehicle[m["vehicle_id"]][m["position"]] = m
    tunnels = {build_tunnel(cfg, i).tunnel_id: build_tunnel(cfg, i) for i in range(5)}
    checked = off = 0
    for msgs in by_vehicle.values():
        if len(msgs) < 3:
            continue
        t = tunnels[msgs["start"]["tunnel_id"]]
        dt_s = abs(msgs["end"]["ts"] - msgs["start"]["ts"]) / 1000
        implied_kmh = t.length_m / dt_s * 3.6
        reported = sum(m["speed_kmh"] for m in msgs.values()) / 3
        off += abs(implied_kmh - reported) > 0.2 * reported   # a degraded sensor reads 5x noisier
        assert (msgs["start"]["ts"] < msgs["end"]["ts"]) == (msgs["start"]["direction"] == START_TO_END)
        checked += 1
    assert checked > 100
    assert off / checked < 0.01


def test_classification_disagreement_and_error_rates():
    cfg = Config.model_validate({"topology": {"tunnels": 20},
                                 "payload": {"include_vehicle_id": True, "include_true_class": True}})
    sink = run_sim(cfg, 300)
    msgs = [m for _, m in sink.messages]
    assert len(msgs) > 10_000
    wrong = sum(m["classification"] != m["true_class"] for m in msgs) / len(msgs)
    assert 0.01 < wrong < 0.15
    per_vehicle = defaultdict(set)
    for m in msgs:
        per_vehicle[m["vehicle_id"]].add(m["classification"])
    assert any(len(c) > 1 for c in per_vehicle.values()), "sensors should sometimes disagree"
    reported = {m["classification"] for m in msgs}
    assert reported <= set(VEHICLE_TYPES) and len(reported) > 2


def test_missed_and_duplicate_detections():
    cfg = noiseless_config(miss_rate=0.2, duplicate_rate=0.1)
    cfg.payload.include_vehicle_id = True
    sink = run_sim(cfg, 300)
    msgs = [m for _, m in sink.messages]
    ids = [m["message_id"] for m in msgs]
    dup_ratio = 1 - len(set(ids)) / len(ids)
    assert 0.05 < dup_ratio < 0.15
    per_vehicle = defaultdict(set)
    for m in msgs:
        per_vehicle[m["vehicle_id"]].add(m["position"])
    incomplete = sum(len(p) < 3 for p in per_vehicle.values()) / len(per_vehicle)
    assert incomplete > 0.3  # 1 - 0.8^3 ≈ 0.49


def test_topics_and_payload_shape():
    cfg = noiseless_config()
    sink = run_sim(cfg, 60)
    topic, m = sink.messages[0]
    assert topic == f"tunnels/{m['tunnel_id']}/sensors/{m['position']}/detections"
    assert set(m) == {"schema", "message_id", "sensor_id", "device_id", "device_serial", "tunnel_id",
                      "position", "seq", "ts", "direction", "lane", "plate", "speed_kmh", "length_m",
                      "occupancy_ms", "classification", "confidence"}
    assert "vehicle_id" not in m


def test_warm_start_populates_tunnels():
    cfg = noiseless_config()
    sink = run_sim(cfg, 1.0)
    assert any(m["position"] != ("start" if m["direction"] == START_TO_END else "end") for _, m in sink.messages)


def test_sharding_covers_all_tunnels_once():
    cfg = noiseless_config()
    ids = []
    for w in range(3):
        ids += [t.tunnel_id for t in Worker(cfg, w, 3, "x", sink=CaptureSink()).tunnels]
    assert sorted(ids) == sorted(build_tunnel(cfg, i).tunnel_id for i in range(20))


# --- config --------------------------------------------------------------

def test_env_overrides_nested_values():
    data = {"mqtt": {"host": "a"}}
    apply_env_overrides(data, {"SIM__MQTT__HOST": "broker", "SIM__MQTT__PORT": "8883",
                               "SIM__SENSORS__OVERRIDES__MIDDLE__MISS_RATE": "0.2",
                               "SIM__TOPOLOGY__LENGTH_M": "[100, 200]", "OTHER": "x"})
    cfg = Config.model_validate(data)
    assert cfg.mqtt.host == "broker" and cfg.mqtt.port == 8883
    assert cfg.sensors.error_model("middle", degraded=False).miss_rate == 0.2
    assert cfg.sensors.error_model("start", degraded=False).miss_rate == 0.01
    assert cfg.topology.length_m == (100, 200)


def test_default_config_file_runs_the_curated_profile_tunnels():
    cfg = load_config("config/simulator.yaml", environ={})
    assert cfg.topology.mode == "profiles"
    assert cfg.tunnel_count() == len(cfg.profiles) == 5


def test_generator_mode_still_targets_high_volume():
    cfg = load_config("config/simulator.yaml", environ={"SIM__TOPOLOGY__MODE": "generator"})
    assert cfg.profiles == []
    assert cfg.expected_msgs_per_s() > 90_000


def test_invalid_config_rejected():
    with pytest.raises(ValueError):
        Config.model_validate({"topology": {"length_m": [500, 100]}})
    with pytest.raises(ValueError):
        Config.model_validate({"mqtt": {"topic_template": "tunnels/{nope}"}})
    with pytest.raises(ValueError):
        Config.model_validate({"mqtt": {"hostname": "typo"}})


def test_metrics_render_and_snapshot():
    """Supervisor metrics: snapshot of worker stats rendered as Prometheus text."""
    from tunnel_sim.__main__ import StatsAggregator
    from tunnel_sim.metrics import METRICS, render

    agg = StatsAggregator(2)
    for worker_id in (0, 1):
        agg.update({
            "worker_id": worker_id, "monotonic": 1.0, "published": 10, "dropped": 1, "missed": 2,
            "duplicates": 3, "vehicles": 4, "clients_connected": 5, "clients": 6, "connected": worker_id == 0,
            "max_lag_s": 0.25 * (worker_id + 1), "pending": 7, "in_flight_events": 8,
        })
    snap = agg.snapshot()
    assert snap["tunnel_sim_published_total"] == 20
    assert snap["tunnel_sim_clients_connected"] == 10 and snap["tunnel_sim_clients"] == 12
    assert snap["tunnel_sim_workers"] == 2 and snap["tunnel_sim_workers_reporting"] == 2
    assert snap["tunnel_sim_workers_disconnected"] == 1
    assert snap["tunnel_sim_max_lag_seconds"] == 0.5
    assert set(snap) <= set(METRICS)

    text = render(snap)
    assert "# TYPE tunnel_sim_published_total counter" in text
    assert "tunnel_sim_published_total 20" in text
    assert text.endswith("\n")


def test_metrics_port_config_override(monkeypatch):
    """SIM__SERVICE__HTTP_PORT switches the endpoint on."""
    from tunnel_sim.config import load_config

    monkeypatch.setenv("SIM__SERVICE__HTTP_PORT", "9109")
    assert load_config(None).service.http_port == 9109
    monkeypatch.delenv("SIM__SERVICE__HTTP_PORT")
    assert load_config(None).service.http_port is None
