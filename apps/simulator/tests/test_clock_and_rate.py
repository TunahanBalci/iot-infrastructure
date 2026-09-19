"""Simulated clock (Europe/Istanbul, host-synced, overridable) and the per-device message rate."""

import heapq
import json
import time
import urllib.request
from datetime import datetime, timedelta, timezone

from tunnel_sim.clock import ISTANBUL, SimClock
from tunnel_sim.config import Config
from tunnel_sim.metrics import MetricsServer
from tunnel_sim.sinks import Sink
from tunnel_sim.worker import Worker

PROFILE = {
    "tunnel_id": "TR-TEST", "name": "Test", "city": "Bolu", "city_code": 14,
    "length_m": 1000.0, "lanes_per_direction": 2, "speed_limit_kmh": 80.0,
    "type_mix": {"OTOMOBIL": 1.0},
}
# 2026-06-15 03:00 Istanbul (quiet) and 17:30 Istanbul (burst)
QUIET = datetime(2026, 6, 15, 3, 0, tzinfo=ISTANBUL).timestamp()
BURST = datetime(2026, 6, 15, 17, 30, tzinfo=ISTANBUL).timestamp()


def config(**overrides) -> Config:
    return Config.model_validate({
        "profiles": [PROFILE],
        "sensors": {"miss_rate": 0, "duplicate_rate": 0, "degraded_ratio": 0},
        **overrides,
    })


class CountingSink(Sink):
    def __init__(self):
        self.count = 0

    def publish(self, sensor, payload):
        self.count += 1
        return True


def messages_per_device_s(cfg: Config, t0_epoch: float, seconds: float) -> float:
    """Drive a worker through `seconds` of simulated time starting at `t0_epoch`."""
    sink = CountingSink()
    w = Worker(cfg, worker_id=0, n_workers=1, boot_id="test", sink=sink, t0_epoch=t0_epoch)
    w.seed_arrivals()
    w.fast_forward()
    while w.heap and w.heap[0][0] <= seconds:
        w._handle(heapq.heappop(w.heap), live=True)
    return sink.count / seconds / 3     # three devices in the tunnel


# --- clock ---------------------------------------------------------------

def test_clock_starts_at_host_time_in_istanbul():
    clock = SimClock(t0_epoch=QUIET)
    assert clock.epoch(0.0) == QUIET
    assert clock.local(0.0).hour == 3
    assert clock.local(0.0).utcoffset() == timedelta(hours=3)
    assert clock.minute_of_day(0.0) == 180


def test_clock_is_istanbul_whatever_the_host_timezone_is(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    # 14:30 UTC is 17:30 in Istanbul
    utc = datetime(2026, 6, 15, 14, 30, tzinfo=timezone.utc).timestamp()
    assert SimClock(t0_epoch=utc).local(0.0).hour == 17


def test_offset_moves_the_simulated_time_and_resync_restores_it():
    clock = SimClock(t0_epoch=QUIET)
    clock.set_offset(6 * 3600)
    assert clock.local(0.0).hour == 9
    assert clock.epoch(0.0) == QUIET + 6 * 3600
    clock.resync()
    assert clock.offset == 0.0 and clock.local(0.0).hour == 3


def test_set_local_time_jumps_to_that_time_of_day():
    clock = SimClock(t0_epoch=QUIET)
    clock.set_local(datetime(2026, 6, 15, 18, 0, tzinfo=ISTANBUL))
    assert clock.local(0.0).hour == 18
    assert clock.offset == 15 * 3600


def test_a_fresh_clock_has_no_offset():
    """A container restart resets an override: nothing about it is persisted."""
    clock = SimClock(t0_epoch=QUIET)
    clock.set_offset(3600)
    assert SimClock(t0_epoch=QUIET).offset == 0.0


# --- rate ----------------------------------------------------------------

def test_each_device_emits_the_configured_rate_outside_the_burst():
    rate = messages_per_device_s(config(), QUIET, seconds=3600)
    assert 1.9 < rate < 2.1


def test_each_device_emits_the_burst_rate_between_17_and_20():
    rate = messages_per_device_s(config(), BURST, seconds=3600)
    assert 2.85 < rate < 3.15


def test_the_burst_window_is_configurable():
    cfg = config(traffic={"msgs_per_device_s": 1.0, "burst": {"from": "03:00", "to": "04:00",
                                                              "msgs_per_device_s": 4.0}})
    assert 3.8 < messages_per_device_s(cfg, QUIET, seconds=1800) < 4.2


def test_the_rate_follows_the_clock_override():
    """Overriding the time to 18:00 puts a quiet-hour simulation into the burst."""
    cfg = config()
    sink = CountingSink()
    w = Worker(cfg, worker_id=0, n_workers=1, boot_id="test", sink=sink, t0_epoch=QUIET)
    w.clock.set_offset(15 * 3600)     # 03:00 -> 18:00
    w.seed_arrivals()
    w.fast_forward()
    while w.heap and w.heap[0][0] <= 1800:
        w._handle(heapq.heappop(w.heap), live=True)
    assert 2.85 < sink.count / 1800 / 3 < 3.15


def test_payload_timestamps_follow_the_clock_override():
    cfg = config()

    class CaptureSink(Sink):
        def __init__(self):
            self.payloads = []

        def publish(self, sensor, payload):
            self.payloads.append(payload)
            return True

    sink = CaptureSink()
    w = Worker(cfg, worker_id=0, n_workers=1, boot_id="test", sink=sink, t0_epoch=QUIET)
    w.clock.set_offset(3600)
    w.seed_arrivals()
    w.fast_forward()
    while w.heap and w.heap[0][0] <= 60 and len(sink.payloads) < 5:
        w._handle(heapq.heappop(w.heap), live=True)
    ts = json.loads(sink.payloads[0])["ts"] / 1000
    assert QUIET + 3600 <= ts <= QUIET + 3600 + 120


# --- control endpoint ----------------------------------------------------

def post(port: int, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read())


def test_time_can_be_set_and_resynced_over_http():
    clock = SimClock(t0_epoch=QUIET)
    server = MetricsServer(0, lambda: {"tunnel_sim_up": 1}, bind="127.0.0.1", clock=clock)
    server.start()
    port = server.port
    try:
        assert post(port, "/time", {"offset_s": 3600})["offset_s"] == 3600
        assert clock.offset == 3600
        assert post(port, "/time", {"set": "2026-06-15T20:00:00"})["local"].startswith("2026-06-15T20:00")
        assert post(port, "/time/resync")["offset_s"] == 0
        assert clock.offset == 0.0
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as r:
            assert b"tunnel_sim_up" in r.read()
    finally:
        server.stop()


def test_a_bad_time_command_is_rejected():
    clock = SimClock(t0_epoch=QUIET)
    server = MetricsServer(0, lambda: {}, bind="127.0.0.1", clock=clock)
    server.start()
    try:
        try:
            post(server.port, "/time", {"set": "not-a-time"})
            raise AssertionError("expected an error response")
        except urllib.error.HTTPError as e:
            assert e.code == 400
        assert clock.offset == 0.0
    finally:
        server.stop()


def test_resync_recovers_from_a_host_suspend():
    """A frozen CLOCK_MONOTONIC (laptop suspend) must not skew payload timestamps forever."""
    w = Worker(config(), 0, 1, "boot", sink=CountingSink())
    t0 = 1_000_000.0
    w.clock.t0_epoch = t0
    threshold = w.cfg.simulation.resync_threshold_s

    # Wall clock advanced 10h past t0 while monotonic only advanced 5s: the host slept.
    mono, mono0 = 105.0, 100.0
    slept = 36_000.0
    now = time.time
    try:
        time.time = lambda: t0 + slept
        assert w._resync(mono, mono0, threshold) == -(slept - 5.0)
        mono0 += w._resync(mono, mono0, threshold)
        # Simulation time now reads the wall clock again, so payload ts are current.
        assert w.clock.epoch(mono - mono0) == t0 + slept
        # And a clock already in sync is left alone.
        assert w._resync(mono, mono0, threshold) == 0.0
    finally:
        time.time = now


def test_resync_ignores_ordinary_jitter():
    w = Worker(config(), 0, 1, "boot", sink=CountingSink())
    w.clock.t0_epoch = 1_000_000.0
    now = time.time
    try:
        time.time = lambda: 1_000_005.5  # 0.5s off a 5s elapsed: scheduling noise, not a suspend
        assert w._resync(105.0, 100.0, 2.0) == 0.0
    finally:
        time.time = now
