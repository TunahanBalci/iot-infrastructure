"""Worker process: owns a shard of tunnels, runs a discrete-event scheduler in real time.

All simulation times inside a worker are seconds relative to the shared simulation
epoch T0 (negative during warm start). They are converted to wall-clock epoch
milliseconds only when a payload is encoded.
"""

from __future__ import annotations

import gc
import heapq
import itertools
import logging
import random
import signal
import time
import traceback
from dataclasses import asdict, dataclass

from .clock import SimClock
from .config import Config
from .model import Sensor, Tunnel, Vehicle, VehicleFactory, build_tunnel, detect
from .vehicles import MIN_SPEED_KMH
from .payload import PayloadEncoder, health_payload
from .sinks import Sink, make_sink

log = logging.getLogger(__name__)

ARRIVAL = 0
SENSOR = 1
DUPLICATE = 2
HEALTH = 3

STOP_CHECK_INTERVAL_S = 0.05
MAX_SLEEP_S = 0.1


@dataclass
class WorkerStats:
    worker_id: int
    published: int = 0
    dropped: int = 0
    missed: int = 0
    duplicates: int = 0
    vehicles: int = 0
    health_reports: int = 0
    max_lag_s: float = 0.0
    pending: int = 0
    in_flight_events: int = 0
    connected: bool = True
    clients: int = 0
    clients_connected: int = 0
    monotonic: float = 0.0


class Worker:
    def __init__(self, cfg: Config, worker_id: int, n_workers: int, boot_id: str, sink: Sink | None = None,
                 t0_epoch: float | None = None, offset_value=None):
        self.cfg = cfg
        self.worker_id = worker_id
        seed = cfg.simulation.seed
        self.rng = random.Random(f"traffic:{seed}:{worker_id}:{n_workers}") if seed is not None else random.Random()
        self.clock = SimClock(time.time() if t0_epoch is None else t0_epoch, offset_value)
        self.factory = VehicleFactory(cfg, self.rng, self.clock)
        self.encoder = PayloadEncoder(cfg.payload, boot_id, self.clock)
        self.dup_delay_s = (cfg.sensors.duplicate_delay_ms[0] / 1000.0, cfg.sensors.duplicate_delay_ms[1] / 1000.0)
        self.tunnels: list[Tunnel] = [build_tunnel(cfg, cfg.topology.index_offset + i)
                                     for i in range(worker_id, cfg.tunnel_count(), n_workers)]
        self.sink = sink if sink is not None else make_sink(cfg, worker_id, n_workers)
        for t in self.tunnels:
            for s in t.sensors:
                self.sink.attach(s)
        self.sink.start()
        self.heap: list[tuple] = []
        self._tiebreak = itertools.count()
        self.stats = WorkerStats(worker_id)
        self.health_interval_s = cfg.service.health_interval_s
        self.device_metrics = cfg.service.device_metrics_enabled(cfg.tunnel_count())
        self._window_start = {t.tunnel_id: 0.0 for t in self.tunnels}
        self._window_vehicles = dict.fromkeys(self._window_start, 0)

    # --- scheduling -------------------------------------------------------

    def _push(self, due: float, kind: int, a: object, b: object = None) -> None:
        heapq.heappush(self.heap, (due, next(self._tiebreak), kind, a, b))

    def seed_arrivals(self) -> None:
        """Schedule first arrival per tunnel. With warm start, arrivals begin in the past
        so tunnels are already populated at T0."""
        slowest_ms = MIN_SPEED_KMH / 3.6
        for t in self.tunnels:
            start = -(t.length_m / slowest_ms) * 1.5 if self.cfg.simulation.warm_start else 0.0
            self._push(start + self.factory.next_arrival_gap(t, start), ARRIVAL, t)
            self._push(self.health_interval_s, HEALTH, t)

    def fast_forward(self) -> None:
        """Process all events before T0 without publishing (warm start)."""
        heap = self.heap
        while heap and heap[0][0] < 0.0:
            self._handle(heapq.heappop(heap), live=False)

    def _handle(self, entry: tuple, live: bool) -> None:
        due, _, kind, a, b = entry
        if kind == SENSOR:
            vehicle: Vehicle = a  # type: ignore[assignment]
            step: int = b  # type: ignore[assignment]
            sensor = vehicle.order[step]
            if live:
                self._observe(sensor, vehicle, due)
            if step < 2:
                self._push(vehicle.time_at(vehicle.order[step + 1]), SENSOR, vehicle, step + 1)
            elif live:
                self.stats.vehicles += 1
                self._window_vehicles[vehicle.tunnel.tunnel_id] += 1
        elif kind == ARRIVAL:
            tunnel: Tunnel = a  # type: ignore[assignment]
            vehicle = self.factory.spawn(tunnel, due)
            self._push(vehicle.time_at(vehicle.order[0]), SENSOR, vehicle, 0)
            self._push(due + self.factory.next_arrival_gap(tunnel, due), ARRIVAL, tunnel)
        elif kind == DUPLICATE:
            if live:
                sensor: Sensor = a  # type: ignore[assignment]
                if self.sink.publish(sensor, b):  # type: ignore[arg-type]
                    self.stats.published += 1
                    self.stats.duplicates += 1
                    sensor.published += 1
                    sensor.w_published += 1
                    sensor.duplicates += 1
                    sensor.w_duplicates += 1
                else:
                    self.stats.dropped += 1
                    sensor.dropped += 1
        else:  # HEALTH
            if live:
                self._report_health(a, due)  # type: ignore[arg-type]
            self._push(due + self.health_interval_s, HEALTH, a)

    def _observe(self, sensor: Sensor, vehicle: Vehicle, due: float) -> None:
        d = detect(sensor, vehicle, self.rng)
        if d is None:
            self.stats.missed += 1
            sensor.missed += 1
            sensor.w_missed += 1
            return
        payload = self.encoder.encode(d)
        if self.sink.publish(sensor, payload):
            self.stats.published += 1
            sensor.published += 1
            sensor.w_published += 1
        else:
            self.stats.dropped += 1
            sensor.dropped += 1
        rate = sensor.err.duplicate_rate
        if rate and self.rng.random() < rate:
            self._push(due + self.rng.uniform(*self.dup_delay_s), DUPLICATE, sensor, payload)

    # --- real-time loop ---------------------------------------------------

    def run(self, t0_epoch: float, should_stop, on_stats, stats_interval_s: float, max_lag_s: float) -> None:
        """Run in real time until should_stop() returns True.

        t0_epoch: wall-clock epoch seconds corresponding to simulation time 0.
        """
        scale = self.cfg.simulation.time_scale
        tick_s = self.cfg.simulation.tick_ms / 1000.0
        self.clock.t0_epoch = t0_epoch
        # Align local monotonic clock with the shared wall-clock epoch.
        mono0 = time.monotonic() - (time.time() - t0_epoch)
        heap, pop, handle = self.heap, heapq.heappop, self._handle
        stats = self.stats
        next_stats = time.monotonic() + stats_interval_s
        next_stop_check = 0.0
        lag_warned = False
        # Only meaningful at real-time speed: a scaled run is supposed to drift from the wall clock.
        resync_s = self.cfg.simulation.resync_threshold_s if scale == 1.0 else 0.0

        while True:
            mono = time.monotonic()
            if mono >= next_stop_check:
                if should_stop():
                    break
                next_stop_check = mono + STOP_CHECK_INTERVAL_S
            if mono >= next_stats:
                if resync_s:
                    mono0 += self._resync(mono, mono0, resync_s)
                self._emit_stats(on_stats)
                lag_warned = False
                next_stats = mono + stats_interval_s

            now = (mono - mono0) * scale
            if heap and heap[0][0] <= now:
                lag = now - heap[0][0]
                if lag > stats.max_lag_s:
                    stats.max_lag_s = lag
                    if lag > max_lag_s and not lag_warned:
                        log.warning("worker %d is %.1fs behind schedule (overloaded? pending=%d)",
                                    self.worker_id, lag, self.sink.pending())
                        lag_warned = True
                # Drain everything due, in bounded batches so stop/stats checks still run.
                for _ in range(4096):
                    if not heap or heap[0][0] > now:
                        break
                    handle(pop(heap), True)
                continue

            # Sleep at least one tick: batches publishes instead of busy-waiting between
            # events that are only microseconds apart. Payload timestamps are unaffected.
            delay = (heap[0][0] - now) / scale if heap else MAX_SLEEP_S
            time.sleep(min(max(delay, tick_s), MAX_SLEEP_S))

        self._emit_stats(on_stats)

    def _resync(self, mono: float, mono0: float, threshold_s: float) -> float:
        """Correction to add to mono0 so simulation time tracks the wall clock again.

        CLOCK_MONOTONIC stops during a host suspend and drifts against a stepped wall clock,
        which would otherwise stamp every payload in the past for the rest of the run.
        """
        drift = (time.time() - self.clock.t0_epoch) - (mono - mono0)
        if abs(drift) < threshold_s:
            return 0.0
        log.warning("event clock was %.1fs %s the wall clock (host suspended or clock stepped); re-anchoring",
                    abs(drift), "behind" if drift > 0 else "ahead")
        return -drift

    def _report_health(self, tunnel: Tunnel, due: float) -> None:
        """Every device of `tunnel` publishes its own condition, retained."""
        tid = tunnel.tunnel_id
        window_s = due - self._window_start[tid]
        ts_ms = int(self.clock.epoch(due) * 1000.0)
        vehicles = self._window_vehicles[tid]
        for sensor in tunnel.sensors:
            payload = health_payload(sensor, ts_ms, window_s, uptime_s=due, tunnel_vehicles=vehicles)
            self.sink.publish_retained(sensor, sensor.health_topic, payload)
            sensor.reset_window()
            self.stats.health_reports += 1
        self._window_start[tid] = due
        self._window_vehicles[tid] = 0

    def _device_stats(self) -> dict:
        return {
            s.sensor_id: {"device_id": s.device_id, "tunnel_id": s.tunnel_id, "position": s.position,
                          "published": s.published, "missed": s.missed, "duplicates": s.duplicates,
                          "dropped": s.dropped, "degraded": s.degraded}
            for t in self.tunnels for s in t.sensors
        }

    def _emit_stats(self, on_stats) -> None:
        s = self.stats
        s.pending = self.sink.pending()
        s.connected = self.sink.connected()
        s.clients_connected, s.clients = self.sink.clients()
        s.in_flight_events = len(self.heap)
        s.monotonic = time.monotonic()
        snapshot = asdict(s)
        snapshot["dropped"] += self.sink.dropped()
        snapshot["devices"] = self._device_stats() if self.device_metrics else {}
        on_stats(snapshot)
        s.max_lag_s = 0.0

    def close(self) -> None:
        self.sink.close()


def worker_main(cfg: Config, worker_id: int, n_workers: int, boot_id: str,
                ready_queue, stats_queue, start_event, stop_event, t0_value, offset_value=None) -> None:
    """multiprocessing entry point."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # supervisor handles Ctrl+C
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    logging.basicConfig(level=cfg.logging.level, format=f"%(asctime)s %(levelname)s [w{worker_id}] %(name)s: %(message)s")
    worker = None
    try:
        worker = Worker(cfg, worker_id, n_workers, boot_id, offset_value=offset_value)
        worker.seed_arrivals()
        worker.fast_forward()
        # Millions of long-lived in-flight objects make full GC passes slow (lag spikes).
        # Freeze the warm-start population and collect young generations less often.
        gc.collect()
        gc.freeze()
        gc.set_threshold(50_000, 20, 100)
        ready_queue.put(("ready", worker_id, len(worker.tunnels), len(worker.heap)))
        while not start_event.wait(0.2):
            if stop_event.is_set():
                return
        worker.run(
            t0_epoch=t0_value.value,
            should_stop=stop_event.is_set,
            on_stats=stats_queue.put,
            stats_interval_s=cfg.simulation.stats_interval_s,
            max_lag_s=cfg.simulation.max_lag_s,
        )
    except ConnectionError as e:
        ready_queue.put(("error", worker_id, str(e)))
        raise SystemExit(1) from None
    except Exception as e:
        ready_queue.put(("error", worker_id, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"))
        raise SystemExit(1) from None
    finally:
        if worker is not None:
            worker.close()
