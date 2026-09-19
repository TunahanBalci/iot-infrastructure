"""Supervisor: loads config, spawns worker processes, aggregates stats, handles shutdown."""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import queue
import secrets
import signal
import sys
import time

import yaml

from .clock import SimClock
from .config import Config, load_config
from .metrics import MetricsServer
from .worker import worker_main

log = logging.getLogger("tunnel_sim")

START_DELAY_S = 0.5
JOIN_TIMEOUT_S = 15.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="tunnel-sim", description="Tunnel vehicle sensor IoT simulator (MQTT 5.0)")
    p.add_argument("-c", "--config", help="config YAML path (default: $SIM_CONFIG or config/simulator.yaml)")
    p.add_argument("--print-config", action="store_true", help="print effective config (file + env overrides) and exit")
    return p.parse_args(argv)


def describe(cfg: Config, n_workers: int) -> None:
    t = cfg.topology
    count = cfg.tunnel_count()
    log.info("mode=%s tunnels=%d devices=%d workers=%d sink=%s broker=%s:%d connection_mode=%s qos=%d time_scale=%.2f",
             t.mode, count, count * 3, n_workers, cfg.output.sink, cfg.mqtt.host, cfg.mqtt.port,
             cfg.mqtt.connection_mode, cfg.mqtt.qos, cfg.simulation.time_scale)
    log.info("expected steady-state rate ≈ %s msg/s (sim time)", f"{cfg.expected_msgs_per_s():,.0f}")


class StatsAggregator:
    """Aggregates cumulative per-worker snapshots; rates use each worker's own snapshot timestamps."""

    RATE_KEYS = ("published", "vehicles")

    def __init__(self, n_workers: int):
        self.latest: dict[int, dict] = {}
        self.rates: dict[int, dict[str, float]] = {}
        self.n_workers = n_workers

    def update(self, s: dict) -> None:
        prev = self.latest.get(s["worker_id"])
        if prev is not None and s["monotonic"] > prev["monotonic"]:
            dt = s["monotonic"] - prev["monotonic"]
            self.rates[s["worker_id"]] = {k: (s[k] - prev[k]) / dt for k in self.RATE_KEYS}
        self.latest[s["worker_id"]] = s

    def device_series(self) -> dict[str, dict[str, float]]:
        """Per-device counters of every worker, keyed by rendered Prometheus labels."""
        series: dict[str, dict[str, float]] = {}
        for worker in self.latest.values():
            for d in worker.get("devices", {}).values():
                labels = (f'device_id="{d["device_id"]}",tunnel_id="{d["tunnel_id"]}",'
                          f'position="{d["position"]}"')
                for metric, key in (("published", "published"), ("missed", "missed"),
                                    ("duplicates", "duplicates"), ("dropped", "dropped")):
                    series.setdefault(f"tunnel_sim_device_{metric}_total", {})[labels] = d[key]
                series.setdefault("tunnel_sim_device_degraded", {})[labels] = float(d["degraded"])
        return series

    def snapshot(self) -> dict[str, float | dict[str, float]]:
        """Current totals, gauges and rates, for metrics.py."""
        workers = list(self.latest.values())
        totals = {k: sum(s[k] for s in workers) for k in ("published", "dropped", "missed", "duplicates", "vehicles")}
        return {
            **self.device_series(),
            "tunnel_sim_up": 1,
            "tunnel_sim_published_total": totals["published"],
            "tunnel_sim_dropped_total": totals["dropped"],
            "tunnel_sim_missed_total": totals["missed"],
            "tunnel_sim_duplicates_total": totals["duplicates"],
            "tunnel_sim_vehicles_total": totals["vehicles"],
            "tunnel_sim_clients_connected": sum(s["clients_connected"] for s in workers),
            "tunnel_sim_clients": sum(s["clients"] for s in workers),
            "tunnel_sim_workers": self.n_workers,
            "tunnel_sim_workers_reporting": len(workers),
            "tunnel_sim_workers_disconnected": sum(1 for s in workers if not s["connected"]),
            "tunnel_sim_max_lag_seconds": max((s["max_lag_s"] for s in workers), default=0.0),
            "tunnel_sim_pending_messages": sum(s["pending"] for s in workers),
            "tunnel_sim_scheduled_vehicles": sum(s["in_flight_events"] for s in workers),
        }

    def report(self) -> None:
        workers = self.latest.values()
        totals = {k: sum(s[k] for s in workers) for k in ("published", "dropped", "missed", "duplicates")}
        rate = {k: sum(r[k] for r in self.rates.values()) for k in self.RATE_KEYS}
        log.info(
            "rate=%s msg/s vehicles=%s/s | total published=%s dropped=%s missed=%s duplicates=%s | "
            "max_lag=%.0fms pending=%s scheduled=%s clients=%s/%s disconnected_workers=%d reporting=%d/%d",
            f"{rate['published']:,.0f}", f"{rate['vehicles']:,.0f}",
            f"{totals['published']:,}", f"{totals['dropped']:,}", f"{totals['missed']:,}", f"{totals['duplicates']:,}",
            max((s["max_lag_s"] for s in workers), default=0.0) * 1000,
            f"{sum(s['pending'] for s in workers):,}", f"{sum(s['in_flight_events'] for s in workers):,}",
            f"{sum(s['clients_connected'] for s in workers):,}", f"{sum(s['clients'] for s in workers):,}",
            sum(1 for s in workers if not s["connected"]), len(self.latest), self.n_workers,
        )


def main(argv: list[str] | None = None) -> int:
    metrics = None
    args = parse_args(argv)
    try:
        cfg = load_config(args.config)
    except Exception as e:
        print(f"invalid configuration: {e}", file=sys.stderr)
        return 2
    if args.print_config:
        print(yaml.safe_dump(cfg.model_dump(mode="json"), sort_keys=False))
        return 0

    logging.basicConfig(level=cfg.logging.level, format="%(asctime)s %(levelname)s [main] %(name)s: %(message)s")
    n_workers = min(cfg.simulation.resolved_workers(), cfg.tunnel_count())
    boot_id = secrets.token_hex(3)
    describe(cfg, n_workers)

    ctx = mp.get_context("spawn")
    ready_q, stats_q = ctx.Queue(), ctx.Queue()
    start_event, stop_event = ctx.Event(), ctx.Event()
    t0 = ctx.Value("d", 0.0)
    # Simulated-time offset, shared with every worker: POST /time moves it, a restart clears it.
    time_offset = ctx.Value("d", 0.0)

    def request_stop(signum, _frame):
        if not stop_event.is_set():
            log.info("received %s, shutting down", signal.Signals(signum).name)
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    procs = [
        ctx.Process(target=worker_main, name=f"worker-{i}", daemon=False,
                    args=(cfg, i, n_workers, boot_id, ready_q, stats_q, start_event, stop_event, t0,
                          time_offset))
        for i in range(n_workers)
    ]
    for p in procs:
        p.start()

    exit_code = 0
    try:
        # Wait for every worker to build its shard, connect and warm up.
        ready = 0
        while ready < n_workers:
            if stop_event.is_set():
                return 130
            try:
                msg = ready_q.get(timeout=0.5)
            except queue.Empty:
                if any(p.exitcode not in (None, 0) for p in procs):
                    log.error("a worker exited during startup")
                    exit_code = 1
                    return exit_code
                continue
            if msg[0] == "error":
                log.error("worker %d failed: %s", msg[1], msg[2])
                exit_code = 1
                return exit_code
            ready += 1
            log.info("worker %d ready: tunnels=%d scheduled_events=%d (%d/%d)", msg[1], msg[2], msg[3], ready, n_workers)

        t0.value = time.time() + START_DELAY_S
        start_event.set()
        log.info("simulation started (boot_id=%s)", boot_id)

        agg = StatsAggregator(n_workers)
        if cfg.service.http_port:
            metrics = MetricsServer(cfg.service.http_port, agg.snapshot, cfg.service.http_bind,
                                    clock=SimClock(t0.value, time_offset))
            metrics.start()
        started = time.monotonic()
        next_report = started + cfg.simulation.stats_interval_s
        duration = cfg.simulation.duration_s
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
            if any(p.exitcode is not None for p in procs):
                log.error("a worker exited unexpectedly")
                exit_code = 1
                break
            now = time.monotonic()
            if now >= next_report:
                agg.report()
                next_report = now + cfg.simulation.stats_interval_s
            if duration is not None and now - started >= duration:
                log.info("duration %.0fs reached", duration)
                break

        stop_event.set()
        deadline = time.monotonic() + JOIN_TIMEOUT_S
        while any(p.is_alive() for p in procs) and time.monotonic() < deadline:
            try:
                agg.update(stats_q.get(timeout=0.2))
            except queue.Empty:
                pass
        while True:
            try:
                agg.update(stats_q.get_nowait())
            except queue.Empty:
                break
        agg.report()
        return exit_code
    finally:
        if metrics is not None:
            metrics.stop()
        stop_event.set()
        for p in procs:
            p.join(timeout=max(0.0, JOIN_TIMEOUT_S))
            if p.is_alive():
                log.warning("terminating unresponsive %s", p.name)
                p.terminate()
                p.join(timeout=5)
        log.info("stopped")


if __name__ == "__main__":
    sys.exit(main())
