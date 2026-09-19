"""Device health: the sensors report their own condition, live as metrics and as retained MQTT."""

import heapq

import orjson

from tunnel_sim.config import Config
from tunnel_sim.metrics import render
from tunnel_sim.sinks import Sink
from tunnel_sim.worker import Worker

PROFILE = {
    "tunnel_id": "TR-TEST", "name": "Test", "city": "Bolu", "city_code": 14,
    "length_m": 1000.0, "lanes_per_direction": 2, "speed_limit_kmh": 80.0,
    "type_mix": {"OTOMOBIL": 1.0},
}


def config(sensors=None, **overrides) -> Config:
    return Config.model_validate({
        "profiles": [PROFILE],
        "sensors": {"miss_rate": 0, "duplicate_rate": 0, "degraded_ratio": 0, **(sensors or {})},
        **overrides,
    })


class RecordingSink(Sink):
    def __init__(self):
        self.detections, self.health = [], []

    def publish(self, sensor, payload):
        self.detections.append((sensor.topic, payload))
        return True

    def publish_retained(self, sensor, topic, payload):
        self.health.append((topic, orjson.loads(payload)))
        return True


def run(cfg: Config, seconds: float) -> RecordingSink:
    sink = RecordingSink()
    w = Worker(cfg, worker_id=0, n_workers=1, boot_id="test", sink=sink, t0_epoch=1_800_000_000.0)
    w.seed_arrivals()
    w.fast_forward()
    while w.heap and w.heap[0][0] <= seconds:
        w._handle(heapq.heappop(w.heap), live=True)
    return sink


# --- health messages -----------------------------------------------------

def test_every_device_reports_health_on_its_own_topic():
    sink = run(config(), 130)
    topics = {t for t, _ in sink.health}
    assert topics == {f"tunnels/TR-TEST/sensors/{p}/health" for p in ("start", "middle", "end")}


def test_health_reports_what_the_device_did_in_the_window():
    sink = run(config(), 130)
    topic, body = sink.health[0]
    assert body["schema"] == 2
    assert body["sensor_id"].startswith("TR-TEST-")
    assert body["device_id"].startswith("02:") and body["device_serial"].startswith("TSN-")
    assert body["tunnel_id"] == "TR-TEST" and body["position"] in ("start", "middle", "end")
    assert body["status"] == "ok" and body["degraded"] is False
    assert body["detections"] > 0 and body["window_s"] > 0
    assert body["missed"] == 0 and body["duplicates"] == 0
    assert body["uptime_s"] >= body["window_s"]


def test_health_is_reported_once_per_interval_per_device():
    cfg = config(service={"health_interval_s": 30.0})
    sink = run(cfg, 121)
    per_device = {}
    for _, body in sink.health:
        per_device.setdefault(body["sensor_id"], []).append(body)
    assert len(per_device) == 3
    assert all(len(reports) == 4 for reports in per_device.values())


def test_a_degraded_device_says_so():
    # miss_rate is multiplied by degraded_multiplier (5x): 0.05 -> a quarter of vehicles missed,
    # not all of them, so the device is degraded rather than silent.
    sink = run(config({"degraded_ratio": 1.0, "miss_rate": 0.05}), 130)
    assert all(body["degraded"] for _, body in sink.health)
    assert all(body["status"] == "degraded" for _, body in sink.health)
    assert any(body["missed"] > 0 for _, body in sink.health)


def test_a_device_that_reports_nothing_is_silent():
    sink = run(config({"overrides": {"middle": {"miss_rate": 1.0}}}), 130)
    by_position = {body["position"]: body for _, body in sink.health}
    assert by_position["middle"]["status"] == "silent"
    assert by_position["middle"]["detections"] == 0
    assert by_position["start"]["status"] == "ok"


def test_duplicates_are_counted_for_the_device_that_sent_them():
    sink = run(config({"duplicate_rate": 0.5}), 130)
    assert sum(body["duplicates"] for _, body in sink.health) > 0


# --- per-device metrics --------------------------------------------------

def test_worker_stats_carry_per_device_counters():
    cfg = config()
    sink = RecordingSink()
    w = Worker(cfg, worker_id=0, n_workers=1, boot_id="test", sink=sink, t0_epoch=1_800_000_000.0)
    w.seed_arrivals()
    w.fast_forward()
    while w.heap and w.heap[0][0] <= 60:
        w._handle(heapq.heappop(w.heap), live=True)
    stats = []
    w._emit_stats(stats.append)
    devices = stats[0]["devices"]
    assert len(devices) == 3
    entry = devices[w.tunnels[0].sensors[0].sensor_id]
    assert entry["published"] > 0
    assert entry["device_id"] == w.tunnels[0].sensors[0].device_id
    assert entry["tunnel_id"] == "TR-TEST" and entry["position"] == "start"


def test_per_device_metrics_are_rendered_with_labels():
    text = render({
        "tunnel_sim_up": 1,
        "tunnel_sim_device_published_total": {
            ('device_id="02:aa:bb:cc:dd:ee",position="start",tunnel_id="TR-TEST"'): 42,
        },
    })
    assert "tunnel_sim_device_published_total{device_id=\"02:aa:bb:cc:dd:ee\"," in text
    assert text.strip().endswith("42")
    assert "# TYPE tunnel_sim_device_published_total counter" in text


def test_per_device_metrics_are_dropped_at_load_test_scale():
    """75000 devices must not become 75000 Prometheus series."""
    cfg = Config.model_validate({"topology": {"mode": "generator", "tunnels": 25_000}})
    assert not cfg.service.device_metrics_enabled(cfg.tunnel_count())
    assert config().service.device_metrics_enabled(1)


def test_supervisor_turns_device_counters_into_labelled_series():
    from tunnel_sim.__main__ import StatsAggregator

    agg = StatsAggregator(1)
    agg.update({
        "worker_id": 0, "published": 10, "dropped": 0, "missed": 1, "duplicates": 0, "vehicles": 3,
        "health_reports": 3, "max_lag_s": 0.0, "pending": 0, "in_flight_events": 0, "connected": True,
        "clients": 1, "clients_connected": 1, "monotonic": 1.0,
        "devices": {"TR-TEST-start": {"device_id": "02:aa:bb:cc:dd:ee", "tunnel_id": "TR-TEST",
                                      "position": "start", "published": 10, "missed": 1,
                                      "duplicates": 0, "dropped": 0, "degraded": True}},
    })
    snap = agg.snapshot()
    labels = 'device_id="02:aa:bb:cc:dd:ee",tunnel_id="TR-TEST",position="start"'
    assert snap["tunnel_sim_device_published_total"][labels] == 10
    assert snap["tunnel_sim_device_missed_total"][labels] == 1
    assert snap["tunnel_sim_device_degraded"][labels] == 1
    assert "tunnel_sim_device_published_total{" + labels + "} 10" in render(snap)
