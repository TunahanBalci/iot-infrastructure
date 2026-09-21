"""Kafka mode throughput: consume + fuse + produce, one consensus process, pre-filled topic.

Needs the harness from test_kafka_rebalance.py running (or any Kafka reachable with the same settings):

    KAFKA_HARNESS_DIR=/tmp/kh tests/integration/kafka-scram.sh up cons-kafka 29392 consensus:cons-secret
    KAFKA_HARNESS_DIR=/tmp/kh python tests/integration/bench_kafka.py --tunnels 500 --seconds 600 --sink kafka

Prints records/s (wall), records per CPU second of the whole process (librdkafka threads
included) and peak RSS.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from simdata import simulate  # noqa: E402
from test_kafka_rebalance import HARNESS, NAME, PASSWORD, PORT, USER, ConsensusProc, _producer  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tunnels", type=int, default=500)
    ap.add_argument("--seconds", type=float, default=600)
    ap.add_argument("--sink", default="kafka", choices=["kafka", "discard"])
    ap.add_argument("--suffix", default=".bench")
    args = ap.parse_args()
    state = Path(os.environ.get("KAFKA_HARNESS_DIR", "/tmp/kafka-harness"))
    conf = {"bootstrap.servers": f"127.0.0.1:{PORT}", "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-512", "sasl.username": USER, "sasl.password": PASSWORD,
            "ssl.ca.location": str(state / NAME / "ca.crt")}
    env = {**os.environ, "KAFKA_HARNESS_DIR": str(state)}
    sfx = args.suffix
    for topic, parts in (("iot.detections", 12), ("iot.vehicles", 12), ("iot.traffic", 12),
                         ("iot.sensor-health", 12), ("iot.consensus.geometry", 1)):
        extra = ["compact"] if topic.endswith("geometry") else []
        subprocess.run([str(HARNESS), "topic", NAME, topic + sfx, str(parts), *extra], env=env, check=True)

    t = time.monotonic()
    payloads = simulate(args.seconds, tunnels=args.tunnels)
    print(f"generated {len(payloads):,} detections in {time.monotonic() - t:.1f}s", flush=True)
    p = _producer(conf)
    t = time.monotonic()
    for i, payload in enumerate(payloads):
        key = orjson.loads(payload)["tunnel_id"]
        while True:
            try:
                p.produce("iot.detections" + sfx, payload, key)
                break
            except BufferError:
                p.poll(0.05)
        if i % 10000 == 0:
            p.poll(0)
    assert p.flush(120) == 0
    print(f"produced in {time.monotonic() - t:.1f}s", flush=True)

    logdir = Path(tempfile.mkdtemp(prefix="consensus-bench-"))
    run_id = int(time.time())
    proc = ConsensusProc(f"bench-{run_id}", conf, logdir, group=f"bench-{run_id}", suffix=sfx,
                         extra_env={"CONSENSUS__KAFKA__DETECTIONS_TOPIC": "iot.detections" + sfx,
                                    "CONSENSUS__OUTPUT__SINK": args.sink, "CONSENSUS__SERVICE__STATS_INTERVAL_S": "1"})
    proc.wait_ready(120)
    start_res = proc.resources()
    t0, first = time.monotonic(), None
    samples = []
    while True:
        m = proc.metrics()
        received = m.get("consensus_received_total", 0)
        now = time.monotonic()
        if received and first is None:
            first = (now, received, proc.resources())
        samples.append((now, received))
        if received >= len(payloads):
            break
        if now - t0 > 1800:
            raise SystemExit("timeout")
        time.sleep(1)
    # metrics refresh every second: +-1 s on the measured wall time
    end_res = proc.resources()
    wall = samples[-1][0] - first[0]
    records = samples[-1][1] - first[1]
    cpu = end_res["cpu_s"] - first[2]["cpu_s"]
    result = {
        "detections": len(payloads), "tunnels": args.tunnels, "sink": args.sink,
        "records_measured": int(records), "wall_s": round(wall, 1),
        "records_per_s": round(records / wall) if wall else None,
        "cpu_s": cpu, "records_per_cpu_s": round(records / cpu) if cpu else None,
        "vehicles": m.get("consensus_vehicles_total"), "pending": m.get("consensus_pending"),
        "rss_peak_mib": end_res["rss_peak_mib"], "rss_idle_start_mib": start_res.get("rss_mib"),
        "log": str(proc.log_path),
    }
    proc.stop()
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
