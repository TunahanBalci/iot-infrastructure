"""Entry point. MQTT input: supervisor that spawns one worker process per partition, aggregates
stats, serves /healthz, /readyz and /metrics, handles shutdown. Kafka input: one engine process
(kafka_service)."""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import queue
import signal
import sys
import threading
import time

import yaml

from .config import Config, load_config
from .sharding import Assignment
from .status import COUNTERS, GAUGES, counter_lines, serve_http
from .worker import worker_main

log = logging.getLogger("tunnel_consensus")

JOIN_TIMEOUT_S = 15.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="tunnel-consensus", description="Tunnel sensor consensus service (MQTT 5.0 / Kafka)")
    p.add_argument("-c", "--config", help="config YAML path (default: $CONSENSUS_CONFIG or config/consensus.yaml)")
    p.add_argument("--print-config", action="store_true", help="print effective config (file + env overrides) and exit")
    p.add_argument("--print-assignment", action="store_true", help="print partitions, client ids and filter counts, then exit")
    return p.parse_args(argv)


class StatsAggregator:
    def __init__(self, n_workers: int, stale_after_s: float):
        self.latest: dict[int, dict] = {}
        self.prev: dict[int, dict] = {}
        self.n_workers = n_workers
        self.stale_after_s = stale_after_s
        self.lock = threading.Lock()

    def update(self, s: dict) -> None:
        with self.lock:
            if s["partition"] in self.latest:
                self.prev[s["partition"]] = self.latest[s["partition"]]
            s["received_at"] = time.monotonic()
            self.latest[s["partition"]] = s

    def ready(self) -> bool:
        now = time.monotonic()
        with self.lock:
            return len(self.latest) == self.n_workers and all(
                s["ready"] and now - s["received_at"] < self.stale_after_s for s in self.latest.values())

    def totals(self) -> dict:
        with self.lock:
            return {k: sum(s.get(k, 0) for s in self.latest.values()) for k in COUNTERS + GAUGES}

    def rate(self, key: str) -> float:
        with self.lock:
            r = 0.0
            for p, s in self.latest.items():
                prev = self.prev.get(p)
                if prev is not None and s["monotonic"] > prev["monotonic"]:
                    r += (s[key] - prev[key]) / (s["monotonic"] - prev["monotonic"])
            return r

    def report(self) -> None:
        t = self.totals()
        accuracy = f" accuracy={t['eval_correct'] / t['eval_vehicles']:.4f} mixed={t['eval_mixed']:,}" \
            if t["eval_vehicles"] else ""
        log.info(
            "in=%s msg/s vehicles=%s/s | received=%s dup=%s lost=%s invalid=%s calibrating=%s | "
            "vehicles=%s (3 sensors %s, 2: %s, 1: %s) pending=%s | tunnels=%s calibrated=%s | "
            "out pending=%s dropped=%s ready=%s%s",
            f"{self.rate('received'):,.0f}", f"{self.rate('vehicles'):,.0f}",
            f"{t['received']:,}", f"{t['duplicates']:,}", f"{t['lost']:,}", f"{t['invalid']:,}", f"{t['calibrating']:,}",
            f"{t['vehicles']:,}", f"{t['vehicles_3']:,}", f"{t['vehicles_2']:,}", f"{t['vehicles_1']:,}",
            f"{t['pending']:,}", f"{t['tunnels']:,}", f"{t['tunnels_calibrated']:,}",
            f"{t['out_pending']:,}", f"{t['dropped']:,}", self.ready(), accuracy,
        )

    def metrics(self) -> str:
        lines = counter_lines(self.totals())
        lines += ["# TYPE consensus_ready gauge", f"consensus_ready {int(self.ready())}"]
        return "\n".join(lines) + "\n"


def print_assignment(cfg: Config, ordinal: int) -> None:
    if cfg.input.source == "kafka":
        k = cfg.kafka
        print(f"kafka: group={k.group_id} topic={k.detections_topic} client_id={k.resolved_client_id()} "
              "(partitions are assigned by the consumer group; partitioning.* is ignored)")
        return
    p = cfg.partitioning
    print(f"replicas={p.replicas} workers={p.workers} partitions={p.partitions} ordinal={ordinal}")
    for w in range(p.workers):
        a = Assignment.for_worker(cfg, ordinal, w)
        filters = a.topic_filters()
        print(f"  worker {w}: partition {a.partition} client_id={a.client_id()} filters={len(filters)} "
              f"first={filters[0] if filters else '-'}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        cfg = load_config(args.config)
        ordinal = cfg.partitioning.resolved_ordinal() if cfg.input.source == "mqtt" else 0
    except Exception as e:
        print(f"invalid configuration: {e}", file=sys.stderr)
        return 2
    if args.print_config:
        dump = cfg.model_dump(mode="json")
        for section, key in (("mqtt", "password"), ("kafka", "sasl_password")):
            if dump[section][key] is not None:
                dump[section][key] = "***"
        print(yaml.safe_dump(dump, sort_keys=False))
        return 0
    if args.print_assignment:
        print_assignment(cfg, ordinal)
        return 0

    if cfg.input.source == "kafka":
        from .kafka_service import run_kafka  # imports confluent_kafka

        return run_kafka(cfg)

    logging.basicConfig(level=cfg.logging.level, format="%(asctime)s %(levelname)s [main] %(name)s: %(message)s")
    p = cfg.partitioning
    n_workers = p.workers
    log.info("instance %d/%d, %d worker(s), %d partition(s) total, broker=%s:%d sink=%s",
             ordinal, p.replicas, n_workers, p.partitions, cfg.mqtt.host, cfg.mqtt.port, cfg.output.sink)

    ctx = mp.get_context("spawn")
    ready_q, stats_q = ctx.Queue(), ctx.Queue()
    stop_event = ctx.Event()

    def request_stop(signum, _frame):
        if not stop_event.is_set():
            log.info("received %s, shutting down", signal.Signals(signum).name)
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    procs = [ctx.Process(target=worker_main, name=f"worker-{w}", args=(cfg, ordinal, w, ready_q, stats_q, stop_event))
             for w in range(n_workers)]
    for proc in procs:
        proc.start()
    agg = StatsAggregator(n_workers, stale_after_s=3 * cfg.service.stats_interval_s)
    server = serve_http(cfg.service.http_port, lambda: all(p.is_alive() for p in procs), agg.ready,
                        agg.metrics) if cfg.service.http_port else None

    exit_code = 0
    try:
        next_report = time.monotonic() + cfg.service.stats_interval_s
        while not stop_event.is_set():
            try:
                agg.update(stats_q.get(timeout=0.2))
            except queue.Empty:
                pass
            try:
                msg = ready_q.get_nowait()
                if msg[0] == "error":
                    log.error("worker %d failed: %s", msg[1], msg[2])
                    exit_code = 1
                    break
            except queue.Empty:
                pass
            if any(proc.exitcode is not None for proc in procs):
                log.error("a worker exited unexpectedly")
                exit_code = 1
                break
            now = time.monotonic()
            if now >= next_report:
                agg.report()
                next_report = now + cfg.service.stats_interval_s
        return exit_code
    finally:
        stop_event.set()
        deadline = time.monotonic() + JOIN_TIMEOUT_S
        while any(proc.is_alive() for proc in procs) and time.monotonic() < deadline:
            try:
                agg.update(stats_q.get(timeout=0.2))
            except queue.Empty:
                pass
        for proc in procs:
            proc.join(timeout=max(0.0, deadline - time.monotonic()))
            if proc.is_alive():
                log.warning("terminating unresponsive %s", proc.name)
                proc.terminate()
                proc.join(timeout=5)
        while True:
            try:
                agg.update(stats_q.get_nowait())
            except queue.Empty:
                break
        agg.report()
        if server is not None:
            server.shutdown()
        log.info("stopped")


if __name__ == "__main__":
    sys.exit(main())
