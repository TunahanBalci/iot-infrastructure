"""Consensus quality against the simulator model, in simulated time (no broker)."""

import heapq
from collections import Counter, defaultdict

import orjson
import pytest

from tunnel_consensus.config import Config
from tunnel_consensus.engine import ConsensusEngine
from tunnel_consensus.geometry import GeometryStore

tunnel_sim = pytest.importorskip("tunnel_sim", reason="needs the simulator: pip install -e ../simulator")
from tunnel_sim.config import Config as SimConfig  # noqa: E402
from tunnel_sim.model import build_tunnel  # noqa: E402
from tunnel_sim.sinks import Sink  # noqa: E402
from tunnel_sim.worker import Worker as SimWorker  # noqa: E402

EPOCH_S = 1_800_000_000.0
SIM_SECONDS = 900
# Evaluate vehicles fully inside [calibrated, still running]: warm-start vehicles and
# vehicles cut off by the end of the simulation have partial detections by construction.
EVAL_FROM_S, EVAL_TO_S = 400, SIM_SECONDS - 250


class CaptureSink(Sink):
    def __init__(self):
        self.payloads = []

    def publish(self, sensor, payload):
        self.payloads.append(payload)
        return True


def simulate(seconds: float, tunnels: int = 12, **sensors):
    cfg = SimConfig.model_validate({
        # generator mode: many tunnels at a steady rate, no daily curve and no access rules
        "topology": {"mode": "generator", "tunnels": tunnels},
        "payload": {"include_vehicle_id": True, "include_true_class": True},
        "sensors": sensors,
    })
    sink = CaptureSink()
    w = SimWorker(cfg, worker_id=0, n_workers=1, boot_id="b00t01", sink=sink, t0_epoch=EPOCH_S)
    w.seed_arrivals()
    w.fast_forward()
    while w.heap and w.heap[0][0] <= seconds:
        w._handle(heapq.heappop(w.heap), live=True)
    return cfg, w, sink.payloads


def run_engine(payloads, cfg: Config | None = None):
    out = []
    engine = ConsensusEngine(cfg or Config(), lambda topic, payload, retain: out.append((topic, orjson.loads(payload), retain)))
    for p in payloads:
        engine.ingest(p)
    engine.flush()
    return engine, out


def evaluate(payloads, out):
    dets = {d["message_id"]: d for d in map(orjson.loads, payloads)}
    truth = defaultdict(list)
    for d in dets.values():
        truth[d["vehicle_id"]].append(d)
    lo, hi = (EPOCH_S + EVAL_FROM_S) * 1000, (EPOCH_S + EVAL_TO_S) * 1000
    vehicles = {vid for vid, ds in truth.items() if all(lo <= d["ts"] <= hi for d in ds)}
    events_of, c = defaultdict(list), Counter()
    for topic, e, _ in out:
        if not topic.endswith("/vehicles"):
            continue
        vids = {dets[m]["vehicle_id"] for m in e["detections"]}
        if not vids & vehicles:
            continue
        c["events"] += 1
        c["mixed"] += len(vids) > 1
        c["correct"] += e["classification"] == e["true_class"]
        c[e["sensors"]] += 1
        for vid in vids:
            events_of[vid].append(e)
    perfect = sum(1 for vid in vehicles
                  if len(events_of[vid]) == 1 and len(events_of[vid][0]["detections"]) == len(truth[vid]))
    return {
        "vehicles": len(vehicles),
        "perfect": perfect / len(vehicles),
        "mixed": c["mixed"] / c["events"],
        "accuracy": c["correct"] / c["events"],
        "three_sensors": c[3] / c["events"],
    }


@pytest.fixture(scope="module")
def default_run():
    sim_cfg, sim, payloads = simulate(SIM_SECONDS)
    engine, out = run_engine(payloads)
    return sim_cfg, sim, payloads, engine, out


def test_learns_tunnel_geometry(default_run):
    sim_cfg, _, _, engine, _ = default_run
    for i in range(sim_cfg.topology.tunnels):
        truth = build_tunnel(sim_cfg, i)
        learned = engine.tunnels[truth.tunnel_id].half
        assert learned is not None, f"{truth.tunnel_id} not calibrated"
        assert learned == pytest.approx(truth.length_m / 2, rel=0.01)


def test_duplicates_removed_nothing_lost(default_run):
    _, sim, _, engine, _ = default_run
    assert engine.stats.duplicates == sim.stats.duplicates
    assert engine.snapshot()["lost"] == 0
    assert engine.stats.pending == 0


def test_fusion_quality(default_run):
    _, _, payloads, _, out = default_run
    q = evaluate(payloads, out)
    assert q["vehicles"] > 2000
    # Traffic all moves at the posted limit, so timing alone leaves candidates ambiguous; the
    # plate a device reads identifies the vehicle and resolves them.
    assert q["perfect"] > 0.99        # each vehicle -> exactly one event with all its detections
    assert q["mixed"] < 0.01
    assert q["three_sensors"] > 0.94
    # A single sensor is wrong ~3% (random flips) plus 1% unreadable; consensus must beat that.
    assert q["accuracy"] > 0.99


def test_fusion_quality_with_many_degraded_sensors():
    _, _, payloads = simulate(SIM_SECONDS, degraded_ratio=0.3)
    _, out = run_engine(payloads)
    q = evaluate(payloads, out)
    assert q["perfect"] > 0.85
    # Ten types, and with degraded_ratio=0.3 a third of the devices flip one reading in six and
    # cannot classify one in twenty: still far better than any single sensor.
    assert q["accuracy"] > 0.94


def test_event_timing_matches_tunnel_length(default_run):
    sim_cfg, _, _, _, out = default_run
    lengths = {t.tunnel_id: t.length_m for t in (build_tunnel(sim_cfg, i) for i in range(sim_cfg.topology.tunnels))}
    checked = 0
    # The tail of the run is the flush, which is full of one- and two-sensor events.
    for topic, v, _ in out:
        if topic.endswith("/vehicles") and v["sensors"] == 3:
            travel_s = (v["ts_exit"] - v["ts_entry"]) / 1000
            assert travel_s * v["speed_kmh"] / 3.6 == pytest.approx(lengths[v["tunnel_id"]], rel=0.02)
            checked += 1
    assert checked > 1000


def test_traffic_window_reports_every_tunnel_by_type():
    """Per-sensor health is reported by the devices themselves; consensus reports traffic."""
    _, _, payloads = simulate(SIM_SECONDS, tunnels=6, degraded_ratio=0)
    out = []
    engine = ConsensusEngine(Config(), lambda topic, payload, retain: out.append((topic, orjson.loads(payload), retain)))
    for p in payloads:
        engine.ingest(p)
    engine.flush()
    engine.report_traffic(SIM_SECONDS)

    traffic = [(p, retain) for topic, p, retain in out if topic.endswith("/traffic")]
    assert not [topic for topic, _, _ in out if topic.endswith("/health")]
    assert len(traffic) == 6
    assert all(retain for _, retain in traffic)
    for t, _ in traffic:
        assert t["vehicles"] > 100 and t["length_m"]
        counted = sum(sum(by_type.values()) for by_type in t["counts"].values())
        assert counted == t["vehicles"]
        assert len({vtype for by_type in t["counts"].values() for vtype in by_type}) > 3


def test_geometry_persists_across_restarts(tmp_path, default_run):
    _, _, payloads, _, _ = default_run
    cfg = Config.model_validate({"geometry": {"state_dir": str(tmp_path)}})
    first, _ = run_engine(payloads, cfg)
    first.save_geometry()
    assert GeometryStore(str(tmp_path), 0).load().keys() == first.tunnels.keys()

    second = ConsensusEngine(cfg, lambda *a: None)
    second.ingest(payloads[0])
    assert second.stats.calibrating == 0
