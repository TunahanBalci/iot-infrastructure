"""Kafka mode end to end against a real broker (Kafka 4.3.1, SASL_SSL + SCRAM-SHA-512 in Docker).

Two consensus processes in one group consume simulator detections (ground truth included)
while one is SIGKILLed, restarted, and the other is stopped with SIGTERM. A third process
in its own group is the baseline. The test checks, from the output topics, that no vehicle
the baseline emitted is missing, that duplicates keep their event_id, that quality matches
the baseline and that learned geometry survives the rebalances (no recalibration).

    pytest -m integration -s tests/integration          # ~4 min, needs docker

Environment (all optional):
    CONSENSUS_IT_KAFKA_NAME=cons-kafka   container / network prefix
    CONSENSUS_IT_KAFKA_PORT=29392        host port
    CONSENSUS_IT_KEEP=1                  leave Kafka running afterwards
    CONSENSUS_IT_EXISTING=1              use an already running harness container (same name/port)
    CONSENSUS_IT_TUNNELS=50  CONSENSUS_IT_SIM_S=900  CONSENSUS_IT_RATE=1500 (records/s)
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

import orjson
import pytest

pytestmark = pytest.mark.integration

ck = pytest.importorskip("confluent_kafka")
pytest.importorskip("tunnel_sim")

APP_DIR = Path(__file__).resolve().parents[2]
HARNESS = Path(__file__).with_name("kafka-scram.sh")
NAME = os.environ.get("CONSENSUS_IT_KAFKA_NAME", "cons-kafka")
PORT = int(os.environ.get("CONSENSUS_IT_KAFKA_PORT", "29392"))
USER, PASSWORD = "consensus", "cons-secret"
TUNNELS = int(os.environ.get("CONSENSUS_IT_TUNNELS", "50"))
SIM_S = float(os.environ.get("CONSENSUS_IT_SIM_S", "900"))
RATE = float(os.environ.get("CONSENSUS_IT_RATE", "1500"))
PARTITIONS = 12
TOPICS = {"iot.detections": PARTITIONS, "iot.vehicles": PARTITIONS, "iot.traffic": PARTITIONS,
          "iot.sensor-health": PARTITIONS, "iot.consensus.geometry": 1}
BASELINE = ".baseline"
CALIBRATED = re.compile(r"tunnel (\S+) calibrated")


def _docker_ok() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


@pytest.fixture(scope="module")
def kafka(tmp_path_factory):
    if not _docker_ok():
        pytest.skip("docker not available")
    state = Path(os.environ.get("KAFKA_HARNESS_DIR", tmp_path_factory.mktemp("kafka-harness")))
    env = {**os.environ, "KAFKA_HARNESS_DIR": str(state)}
    existing = os.environ.get("CONSENSUS_IT_EXISTING") == "1"
    if not existing:
        subprocess.run([str(HARNESS), "up", NAME, str(PORT), f"{USER}:{PASSWORD}"], env=env, check=True)
    for suffix in ("", BASELINE):
        for topic, parts in TOPICS.items():
            compact = ["compact"] if topic.endswith("geometry") else []
            subprocess.run([str(HARNESS), "topic", NAME, topic + suffix, str(parts), *compact], env=env, check=True)
    conf = {"bootstrap.servers": f"127.0.0.1:{PORT}", "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-512", "sasl.username": USER, "sasl.password": PASSWORD,
            "ssl.ca.location": str(state / NAME / "ca.crt")}
    yield conf
    if not existing and os.environ.get("CONSENSUS_IT_KEEP") != "1":
        subprocess.run([str(HARNESS), "down", NAME], env=env, check=False)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ConsensusProc:
    def __init__(self, name: str, conf: dict, logdir: Path, group: str = "consensus", suffix: str = "",
                 extra_env: dict[str, str] | None = None):
        self.name = name
        self.port = _free_port()
        self.log_path = logdir / f"{name}.log"
        env = {
            **os.environ,
            "CONSENSUS__INPUT__SOURCE": "kafka",
            "CONSENSUS__OUTPUT__SINK": "kafka",
            "CONSENSUS__KAFKA__BOOTSTRAP_SERVERS": conf["bootstrap.servers"],
            "CONSENSUS__KAFKA__SASL_PASSWORD": conf["sasl.password"],
            "CONSENSUS__KAFKA__SSL_CA_LOCATION": conf["ssl.ca.location"],
            "CONSENSUS__KAFKA__CLIENT_ID": name,
            "CONSENSUS__KAFKA__GROUP_ID": group,
            "CONSENSUS__KAFKA__VEHICLES_TOPIC": "iot.vehicles" + suffix,
            "CONSENSUS__KAFKA__TRAFFIC_TOPIC": "iot.traffic" + suffix,
            "CONSENSUS__KAFKA__SENSOR_HEALTH_TOPIC": "iot.sensor-health" + suffix,
            "CONSENSUS__KAFKA__GEOMETRY_TOPIC": "iot.consensus.geometry" + suffix,
            "CONSENSUS__KAFKA__COMMIT_INTERVAL_S": "2",
            "CONSENSUS__KAFKA__CONSUMER__SESSION_TIMEOUT_MS": "10000",
            "CONSENSUS__GEOMETRY__SAVE_INTERVAL_S": "10",
            "CONSENSUS__HEALTH__INTERVAL_S": "20",
            "CONSENSUS__SERVICE__HTTP_PORT": str(self.port),
            "HOSTNAME": name,
            "OTEL_EXPORTER_OTLP_ENDPOINT": "",
            **(extra_env or {}),
        }
        self.log = self.log_path.open("ab")
        self.started = time.monotonic()
        self.proc = subprocess.Popen([sys.executable, "-m", "tunnel_consensus"], cwd=APP_DIR, env=env,
                                     stdout=self.log, stderr=subprocess.STDOUT)

    def get(self, path: str) -> tuple[int, str]:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=2) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, ""
        except OSError:
            return 0, ""

    def metrics(self) -> dict[str, float]:
        code, text = self.get("/metrics")
        out = {}
        for line in text.splitlines():
            if line and not line.startswith("#"):
                name, _, value = line.rpartition(" ")
                out[name] = float(value)
        return out

    def wait_ready(self, timeout: float = 60) -> float:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"{self.name} exited: {self.log_path.read_text()[-3000:]}")
            if self.get("/readyz")[0] == 200:
                return time.monotonic() - self.started
            time.sleep(0.2)
        raise AssertionError(f"{self.name} not ready after {timeout}s")

    def stop(self, sig=signal.SIGTERM, timeout: float = 60) -> tuple[int, float]:
        t = time.monotonic()
        self.proc.send_signal(sig)
        code = self.proc.wait(timeout)
        self.log.close()
        return code, time.monotonic() - t

    def resources(self) -> dict:
        """Peak/current RSS (MiB) and CPU seconds so far (Linux /proc)."""
        try:
            status = Path(f"/proc/{self.proc.pid}/status").read_text()
            stat = Path(f"/proc/{self.proc.pid}/stat").read_text().rsplit(")", 1)[1].split()
        except OSError:
            return {}
        kb = {line.split(":")[0]: int(line.split()[1]) for line in status.splitlines() if line.startswith("VmHWM") or
              line.startswith("VmRSS")}
        ticks = os.sysconf("SC_CLK_TCK")
        return {"rss_peak_mib": round(kb["VmHWM"] / 1024, 1), "rss_mib": round(kb["VmRSS"] / 1024, 1),
                "cpu_s": round((int(stat[11]) + int(stat[12])) / ticks, 1),
                "wall_s": round(time.monotonic() - self.started, 1)}

    def calibrations(self) -> list[str]:
        return CALIBRATED.findall(self.log_path.read_text(errors="replace"))


def _producer(conf: dict):
    return ck.Producer({**conf, "partitioner": "murmur2_random", "linger.ms": 5, "compression.type": "lz4",
                        "acks": "all", "enable.idempotence": True})


def produce_paced(conf: dict, payloads: list[bytes], rate: float, stats: dict) -> None:
    p = _producer(conf)
    t0 = time.monotonic()
    for i, payload in enumerate(payloads):
        if i % 100 == 0:
            ahead = i / rate - (time.monotonic() - t0)
            if ahead > 0:
                time.sleep(ahead)
            p.poll(0)
        key = orjson.loads(payload)["tunnel_id"]
        while True:
            try:
                p.produce("iot.detections", payload, key)
                break
            except BufferError:
                p.poll(0.05)
    assert p.flush(60) == 0
    stats["produced"] = len(payloads)
    stats["seconds"] = time.monotonic() - t0


def produce_end_markers(conf: dict, last_ts_ms: int) -> None:
    """One far-future detection per partition: advances every partition's event clock so the
    last vehicles are emitted (a fresh uncalibrated tunnel, it never produces an event)."""
    p = _producer(conf)
    for part in range(PARTITIONS):
        tid = f"ZEND{part:02d}"
        body = {"schema": 1, "message_id": f"{tid}-start:end:1", "sensor_id": f"{tid}-start", "tunnel_id": tid,
                "position": "start", "seq": 1, "ts": last_ts_ms + 3_600_000, "direction": "start_to_end", "lane": 1,
                "speed_kmh": 80.0, "length_m": 4.0, "occupancy_ms": 180, "classification": "car", "confidence": 0.9}
        p.produce("iot.detections", orjson.dumps(body), tid, partition=part)
    assert p.flush(30) == 0


def read_all(conf: dict, topic: str) -> list[tuple[str | None, dict | None]]:
    c = ck.Consumer({**conf, "group.id": "it-reader", "enable.auto.commit": False, "enable.partition.eof": True})
    md = c.list_topics(topic, timeout=10).topics[topic]
    parts = sorted(md.partitions)
    ends = {p: c.get_watermark_offsets(ck.TopicPartition(topic, p), timeout=10) for p in parts}
    todo = {p for p, (lo, hi) in ends.items() if hi > lo}
    c.assign([ck.TopicPartition(topic, p, ck.OFFSET_BEGINNING) for p in parts])
    out = []
    deadline = time.monotonic() + 120
    while todo and time.monotonic() < deadline:
        for m in c.consume(5000, 1.0):
            if m.error() is not None:
                if m.error().code() == ck.KafkaError._PARTITION_EOF and m.offset() >= ends[m.partition()][1]:
                    todo.discard(m.partition())
                continue
            out.append((m.key().decode() if m.key() else None, orjson.loads(m.value()) if m.value() else None))
            if m.offset() + 1 >= ends[m.partition()][1]:
                todo.discard(m.partition())
    c.close()
    assert not todo, f"could not read {topic} to the end"
    return out


def wait_drained(procs: list[ConsensusProc], expected_received: int, timeout: float = 180) -> None:
    """Every partition owned by someone, nothing lagging or replaying, receive counters stable."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        ms = [p.metrics() for p in procs]
        if all(ms):
            owned = sum(m.get("consensus_kafka_assigned_partitions", 0) for m in ms)
            lag = sum(m.get("consensus_kafka_lag_records", 1) for m in ms)
            replaying = sum(m.get("consensus_kafka_replaying_partitions", 1) for m in ms)
            received = tuple(m.get("consensus_received_total", 0) for m in ms)
            if owned == PARTITIONS and lag == 0 and replaying == 0 and received == last:
                return
            last = received
        time.sleep(6)   # metrics refresh every stats interval (5 s)
    raise AssertionError(f"not drained after {timeout}s: {[p.metrics() for p in procs]}")


def evaluate(payloads: list[bytes], events: list[dict], from_ms: float, to_ms: float) -> dict:
    """Perfect fusion and accuracy over vehicles fully inside [from_ms, to_ms] (events deduplicated by id)."""
    dets = {d["message_id"]: d for d in map(orjson.loads, payloads)}
    truth = defaultdict(list)
    for d in dets.values():
        truth[d["vehicle_id"]].append(d)
    vehicles = {v for v, ds in truth.items() if all(from_ms <= d["ts"] <= to_ms for d in ds)}
    unique = {e["event_id"]: e for e in events}
    of_vehicle = defaultdict(list)
    c = Counter()
    for e in unique.values():
        vids = {dets[m]["vehicle_id"] for m in e["detections"] if m in dets}
        if not vids & vehicles:
            continue
        c["events"] += 1
        c["mixed"] += len(vids) > 1
        c["correct"] += e["classification"] == e.get("true_class")
        for v in vids:
            of_vehicle[v].append(e)
    perfect = sum(1 for v in vehicles if len(of_vehicle[v]) == 1 and len(of_vehicle[v][0]["detections"]) == len(truth[v]))
    return {"vehicles": len(vehicles), "perfect": round(perfect / len(vehicles), 4),
            "mixed": round(c["mixed"] / c["events"], 4), "accuracy": round(c["correct"] / c["events"], 4)}


def test_rebalances_lose_nothing(kafka, tmp_path):
    from simdata import simulate

    payloads = simulate(SIM_S, tunnels=TUNNELS)
    ts = [orjson.loads(p)["ts"] for p in payloads]
    t_first, t_last = min(ts), max(ts)
    logdir = Path(os.environ.get("CONSENSUS_IT_LOGDIR", tmp_path))
    logdir.mkdir(parents=True, exist_ok=True)
    report: dict = {"detections": len(payloads), "tunnels": TUNNELS, "sim_s": SIM_S, "rate": RATE, "logs": str(logdir)}
    print(f"\n{len(payloads):,} detections, {TUNNELS} tunnels, {SIM_S:.0f} simulated s, logs in {logdir}")

    baseline = ConsensusProc("baseline", kafka, logdir, group="consensus-baseline", suffix=BASELINE)
    a = ConsensusProc("consensus-a", kafka, logdir)
    b = ConsensusProc("consensus-b", kafka, logdir)
    for p in (baseline, a, b):
        report[f"ready_s_{p.name}"] = round(p.wait_ready(), 1)

    stats: dict = {}
    producer = threading.Thread(target=produce_paced, args=(kafka, payloads, RATE, stats))
    t0 = time.monotonic()
    producer.start()
    total_s = len(payloads) / RATE
    timeline = []

    def at(fraction: float) -> None:
        delay = t0 + fraction * total_s - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    at(0.35)
    code, _ = b.stop(signal.SIGKILL)
    timeline.append(f"{time.monotonic() - t0:.0f}s SIGKILL consensus-b")
    at(0.60)
    b2 = ConsensusProc("consensus-b2", kafka, logdir)
    report["ready_s_consensus-b2"] = round(b2.wait_ready(), 1)
    timeline.append(f"{time.monotonic() - t0:.0f}s restarted consensus-b2")
    at(0.85)
    m_a = a.metrics()
    report["resources_consensus_a"] = a.resources()
    code, took = a.stop(signal.SIGTERM)
    timeline.append(f"{time.monotonic() - t0:.0f}s SIGTERM consensus-a (exit {code} in {took:.1f}s)")
    report["sigterm_exit_code"], report["sigterm_shutdown_s"] = code, round(took, 2)
    producer.join()
    produce_end_markers(kafka, t_last)
    timeline.append(f"{time.monotonic() - t0:.0f}s produced {stats['produced']:,} detections + end markers")

    wait_drained([b2], len(payloads))
    wait_drained([baseline], len(payloads))
    m_b2, m_base = b2.metrics(), baseline.metrics()
    report["resources_consensus_b2"], report["resources_baseline"] = b2.resources(), baseline.resources()
    for p in (b2, baseline):
        code, took = p.stop(signal.SIGTERM)
        assert code == 0
    timeline.append(f"{time.monotonic() - t0:.0f}s drained and stopped")
    report["timeline"] = timeline

    dets = {d["message_id"]: d for d in map(orjson.loads, payloads)}
    base_records = read_all(kafka, "iot.vehicles" + BASELINE)
    chaos_records = read_all(kafka, "iot.vehicles")
    assert all(k == v["tunnel_id"] for k, v in base_records + chaos_records)
    base_events = [v for _, v in base_records]
    chaos_events = [v for _, v in chaos_records]

    def ids_by_vehicle(events):
        out = defaultdict(set)
        for e in events:
            for m in e["detections"]:
                out[dets[m]["vehicle_id"]].add(e["event_id"])
        return out

    base_ids, chaos_ids = ids_by_vehicle(base_events), ids_by_vehicle(chaos_events)
    eval_from = t_first + 300_000   # after calibration
    eval_vehicles = {dets[m]["vehicle_id"] for e in base_events if e["ts_entry"] >= eval_from for m in e["detections"]}
    lost = sorted(v for v in eval_vehicles if v not in chaos_ids)
    counts = Counter(e["event_id"] for e in chaos_events)
    same_id_dups = sum(n - 1 for n in counts.values() if n > 1)
    different_id = sorted(v for v in eval_vehicles if chaos_ids[v] - base_ids[v])

    geometry = {k: v for k, v in read_all(kafka, "iot.consensus.geometry")}
    geometry = {k: v for k, v in geometry.items() if v is not None}
    chaos_procs = (a, b, b2)
    calibrations = Counter(t for p in chaos_procs for t in p.calibrations())
    q_from, q_to = t_first + 400_000, t_last - 250_000
    report.update({
        "baseline_events": len(base_events), "chaos_events": len(chaos_events),
        "eval_vehicles": len(eval_vehicles), "lost": len(lost), "lost_sample": lost[:10],
        "same_event_id_duplicates": same_id_dups, "vehicles_with_other_event_id": len(different_id),
        "other_event_id_sample": [(v, sorted(base_ids[v]), sorted(chaos_ids[v])) for v in different_id[:5]],
        "quality_baseline": evaluate(payloads, base_events, q_from, q_to),
        "quality_rebalanced": evaluate(payloads, chaos_events, q_from, q_to),
        "geometry_topic_tunnels": len(geometry),
        "calibrations_total": sum(calibrations.values()), "recalibrated_tunnels": sorted(t for t, n in calibrations.items() if n > 1),
        "calibrations_per_process": {p.name: len(p.calibrations()) for p in chaos_procs},
        "metrics_consensus_b2": {k: v for k, v in m_b2.items() if k.startswith(("consensus_kafka", "consensus_geometry",
                                 "consensus_calibrating", "consensus_replay", "consensus_received", "consensus_vehicles_total"))},
        "metrics_consensus_a_before_sigterm": {k: v for k, v in m_a.items() if k.startswith(("consensus_kafka_rebalances",
                                               "consensus_replay", "consensus_geometry", "consensus_calibrating"))},
        "metrics_baseline": {k: v for k, v in m_base.items() if k in ("consensus_received_total", "consensus_vehicles_total",
                                                                      "consensus_calibrating_total")},
    })
    (logdir / "report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))

    assert not lost, f"{len(lost)} vehicles lost"
    # Around a handover the new owner lacks the learned sensor noise: in dense, ambiguous traffic
    # a few vehicles are fused differently than in an uninterrupted run (not lost).
    assert len(different_id) <= 0.01 * len(eval_vehicles)
    assert report["quality_rebalanced"]["perfect"] >= report["quality_baseline"]["perfect"] - 0.01
    assert report["quality_rebalanced"]["accuracy"] >= 0.99
    assert len(geometry) == TUNNELS
    assert not report["recalibrated_tunnels"], "a rebalance triggered recalibration"
    assert report["sigterm_exit_code"] == 0
