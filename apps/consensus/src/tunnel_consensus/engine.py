"""Consensus engine: fuses the start/middle/end detections of one vehicle into one event.

Pipeline per detection (all state is per tunnel; a tunnel is owned by exactly one partition):

  1. dedup      drop repeated (sensor, boot_id, seq); count seq gaps as lost messages
  2. geometry   until the tunnel's sensor spacing is learned, feed the calibrator only
  3. pend       detections at the first and middle sensor (in travel order) wait, per
                direction and lane, until the vehicle reaches the last sensor
  4. associate  a detection at the last sensor picks its partners. Detections that read the
                same plate are the same vehicle and pair directly; for the rest (and for
                devices that read no plate) the timing does it: at constant speed the middle
                detection lies at the midpoint of first and last, a constraint that only
                carries timestamp jitter. Without a triplet it falls back to the best
                consistent pair, else it stands alone.
  5. expire     a pending detection whose vehicle should have reached the last sensor by
                now (event-time watermark + lateness) pairs with a pending partner or
                stands alone: the last sensor missed it
  6. fuse       weighted vote over the vehicle types: every sensor contributes the log-odds of
                its own reported confidence to the type it reported, and BILINMEYEN abstains.
                Speed and length noise are still learned per sensor (the association gates use
                them); the devices report their own health, so nothing here judges them.

The engine is single-threaded and does no network I/O: output goes through a sink
(`TopicSink` wraps the MQTT-style `emit(topic, payload, retain)`).

Kafka input runs one engine per assigned partition. The engine then also tracks the
offsets of pending detections (commit low watermark) and can replay the records between
the committed offset and the previous owner's position without re-emitting what that
owner already emitted (see `begin_replay`).
"""

from __future__ import annotations

import heapq
import itertools
import logging
import math
import time
from bisect import bisect_left, bisect_right, insort
from collections.abc import Callable
from dataclasses import asdict, dataclass
from operator import attrgetter
from typing import Protocol

import orjson

from .config import Config, OutputConfig
from .geometry import Calibrator, GeometryParams, GeometryStore
from .model import (
    POSITION_AT,
    SCHEMA_VERSION,
    UNKNOWN,
    VEHICLE_TYPES,
    Detection,
    InvalidDetection,
    SensorState,
    parse_detection,
)

log = logging.getLogger(__name__)

# emit(topic, payload, retain)
Emit = Callable[[str, bytes, bool], None]


class Sink(Protocol):
    """Engine outputs. Every record belongs to one tunnel (Kafka key)."""

    def vehicle(self, tunnel_id: str, payload: bytes, event: dict, dets: list[Detection]) -> None: ...

    def traffic(self, tunnel_id: str, payload: bytes) -> None: ...


class TopicSink:
    """MQTT-style outputs: topic templates, reports retained."""

    __slots__ = ("emit", "vehicle_topic", "traffic_topic")

    def __init__(self, emit: Emit, o: OutputConfig):
        self.emit = emit
        self.vehicle_topic = o.vehicle_topic_template
        self.traffic_topic = o.traffic_topic_template

    def vehicle(self, tunnel_id: str, payload: bytes, event: dict, dets: list[Detection]) -> None:
        self.emit(self.vehicle_topic.format(tunnel_id=tunnel_id), payload, False)

    def traffic(self, tunnel_id: str, payload: bytes) -> None:
        self.emit(self.traffic_topic.format(tunnel_id=tunnel_id), payload, True)

# Classes a vote can land on (BILINMEYEN abstains, so it is not one of them).
N_CLASSES_MINUS_1 = float(len(VEHICLE_TYPES) - 2)
IDLE_ADVANCE_S = 1.0      # advance the watermark on wall time after this long without input
MAX_SPEED_NOISE = 0.5     # cap on a learned relative speed error
MAX_LENGTH_NOISE_M = 10.0
REFINE_CLIP = 0.05        # geometry refinement ignores vehicles implying > 5% different spacing
SEARCH_EXTRA_SIGMA = 2.0  # candidate search and expiry use a wider window than the scoring gate
# Chi-square-like score limits (~99.9% of true matches): a worse "best" candidate is a
# coincidence in dense traffic, typically because the true partner was missed.
TRIPLET_MAX_SCORE = 25.0  # midpoint + 3 speeds + lengths
PAIR_MAX_SCORE = 18.0     # travel time + speed difference + length difference
# End of a Kafka replay: a replayed pending detection whose deadline had passed the previous
# owner's clock by this margin was already emitted by that owner. Inside the margin it is kept
# (a possible duplicate, never a loss).
# The deadline uses twice the learned speed noise: the noise learned during the replay can be
# lower than what the previous owner had learned when it computed its deadline.
REPLAY_MARGIN_S = 2.0
REPLAY_MARGIN_RATIO = 0.05   # ... plus this share of the remaining travel time
REPLAY_NOISE_FACTOR = 2.0
EVENT_BUCKET_BITS = 8        # replay_start(): event start offsets kept per 256 offsets of their end
_TS = attrgetter("ts")


@dataclass
class EngineStats:
    received: int = 0
    invalid: int = 0
    foreign: int = 0          # detections for tunnels owned by another partition
    duplicates: int = 0
    lost: int = 0             # seq gaps across all sensors (QoS 0 loss upstream)
    calibrating: int = 0      # detections consumed while a tunnel's geometry was unknown
    vehicles: int = 0
    vehicles_3: int = 0       # ... fused from all three sensors
    vehicles_2: int = 0
    vehicles_1: int = 0
    pending: int = 0          # detections waiting for their vehicle to reach the last sensor
    tunnels: int = 0
    tunnels_calibrated: int = 0
    # simulator evaluation mode (payload.include_true_class / include_vehicle_id)
    eval_vehicles: int = 0
    eval_correct: int = 0
    eval_mixed: int = 0       # events that fused detections of different vehicles
    # Kafka replay after a partition moved here: vehicles the previous owner already emitted
    replay_suppressed: int = 0    # ... fused again but not emitted
    replay_dropped: int = 0       # ... detections still pending when the replay ended, dropped


class TunnelState:
    __slots__ = ("tunnel_id", "half", "calibrator", "warm_until", "mid_var", "sensors", "lanes",
                 "w_counts", "w_speed_sum", "w_vehicles")

    def __init__(self, tunnel_id: str, half: float | None, mid_var: float):
        self.tunnel_id = tunnel_id
        self.half = half                                   # sensor spacing L/2 (m); None = calibrating
        self.calibrator = None if half is not None else Calibrator()
        self.warm_until = 0.0                              # misses before this ts are not counted
        self.mid_var = mid_var                             # learned variance of the midpoint residual (s^2)
        self.sensors: dict[str, SensorState] = {}          # position -> state
        # (direction, lane) -> [pending at travel index 0, pending at travel index 1], sorted by ts
        self.lanes: dict[tuple[str, int], tuple[list[Detection], list[Detection]]] = {}
        self.reset_window()

    def reset_window(self) -> None:
        # direction -> {vehicle type: count} for the vehicles fused in this window
        self.w_counts = {"start_to_end": {}, "end_to_start": {}}
        self.w_speed_sum = 0.0
        self.w_vehicles = 0


def _compatible(candidates: list[Detection], plate: str) -> list[Detection]:
    """Candidates that can be the same vehicle as a detection that read `plate`."""
    if not plate:
        return candidates
    return [d for d in candidates if not d.plate or d.plate == plate]


def _plate_of(dets: list[Detection]) -> str:
    """The plate of the fused vehicle: candidates never disagree, but one may have read none."""
    for d in dets:
        if d.plate:
            return d.plate
    return ""


def _window(items: list[Detection], lo: float, hi: float) -> list[Detection]:
    return items[bisect_left(items, lo, key=_TS):bisect_right(items, hi, key=_TS)]


class ConsensusEngine:
    def __init__(self, cfg: Config, emit: Emit | None = None, partition: int = 0,
                 owns: Callable[[str], bool] | None = None, *, sink: Sink | None = None,
                 known_halves: dict[str, float] | None = None, track_offsets: bool = False,
                 on_calibrated: Callable[[str, float], None] | None = None):
        """emit/sink: outputs. known_halves: learned geometry to start from (default: the file store).
        track_offsets: Kafka input, detections carry offsets (see watermark()).
        on_calibrated(tunnel_id, half): called when a tunnel's geometry is first learned."""
        self.cfg = cfg
        if sink is None:
            if emit is None:
                raise ValueError("ConsensusEngine needs emit or sink")
            sink = TopicSink(emit, cfg.output)
        self.sink = sink
        self.owns = owns
        self.on_calibrated = on_calibrated
        a, f = cfg.association, cfg.fusion
        self.geo = GeometryParams(cfg.geometry)
        self.refine_alpha = cfg.geometry.refine_alpha
        if known_halves is None:
            self.store = GeometryStore(cfg.geometry.state_dir, partition)
            self.known_halves = self.store.load()
        else:
            self.store = GeometryStore(None, partition)
            self.known_halves = known_halves   # shared, kept up to date by the owner
        # Kafka: offsets of pending detections, insertion (= offset) order; next offset to consume.
        self.pending_offsets: dict[int, None] | None = {} if track_offsets else None
        self.next_offset = -1
        # Kafka: oldest detection offset of emitted events, bucketed by their newest detection offset.
        self.event_starts: dict[int, int] | None = {} if track_offsets else None
        # Kafka replay (begin_replay): suppress output until offset replay_until.
        self.replay_until: int | None = None
        self.replay_pending_from = 0
        self.replay_clock = 0.0
        self.suppress = False
        self.deadline_var_floor = 0.0
        self.warm_until_floor = 0.0
        self.mid_sigma_min = a.midpoint_tolerance_ms / 1000.0 / a.gate_sigma
        self.tol_fixed = a.time_tolerance_ms / 1000.0
        self.tol_ratio = a.time_tolerance_ratio
        self.gate_sigma = a.gate_sigma
        self.search_sigma = a.gate_sigma + SEARCH_EXTRA_SIGMA
        self.speed_noise_prior = a.speed_noise_prior
        self.length_noise_prior = a.length_noise_prior_m
        self.noise_alpha = a.noise_alpha
        self.lateness = a.max_lateness_ms / 1000.0
        self.dedup_window = a.dedup_window
        self.min_speed_ms = cfg.geometry.speed_kmh[0] / 3.6
        self.position_weights = f.position_weights
        self.conf_lo, self.conf_hi = f.confidence_range
        self.tunnels: dict[str, TunnelState] = {}
        self.expiry: list[tuple[float, int, Detection, TunnelState]] = []
        self.seq = itertools.count()
        self.clock = 0.0            # event time: max detection ts seen (+ idle wall time)
        self.last_input_wall = 0.0
        self.stats = EngineStats()
        self.include_detections = cfg.output.include_detections
        if self.known_halves and known_halves is None:
            log.info("loaded learned geometry for %d tunnels", len(self.known_halves))

    # --- input ------------------------------------------------------------

    def ingest(self, payload: bytes, wall_now: float = 0.0, offset: int = -1, trace: bytes | None = None) -> None:
        """offset/trace: Kafka record offset and sampled traceparent (Kafka input only)."""
        if offset >= 0:
            if self.replay_until is not None:
                if offset >= self.replay_until:
                    self.end_replay()
                elif offset >= self.replay_pending_from:
                    # From here on the previous owner may still have held detections pending.
                    self.deadline_var_floor = MAX_SPEED_NOISE ** 2
            self.next_offset = offset + 1
        self.stats.received += 1
        try:
            d = parse_detection(payload)
        except InvalidDetection as e:
            self.stats.invalid += 1
            if self.stats.invalid <= 10 or self.stats.invalid % 10000 == 0:
                log.warning("invalid detection (%d so far): %s", self.stats.invalid, e)
            return
        if offset >= 0:
            d.offset = offset
            d.trace = trace
        self.process(d, wall_now)

    def _sensor(self, tunnel: TunnelState, position: str, sensor_id: str | None = None) -> SensorState:
        s = tunnel.sensors.get(position)
        if s is None:
            s = tunnel.sensors[position] = SensorState(
                sensor_id or f"{tunnel.tunnel_id}-{position}", position, self.dedup_window,
                self.speed_noise_prior, self.length_noise_prior)
        return s

    def process(self, d: Detection, wall_now: float = 0.0) -> None:
        if self.owns is not None and not self.owns(d.tunnel_id):
            self.stats.foreign += 1
            return
        self.last_input_wall = wall_now
        tunnel = self.tunnels.get(d.tunnel_id)
        if tunnel is None:
            tunnel = self.tunnels[d.tunnel_id] = TunnelState(
                d.tunnel_id, self.known_halves.get(d.tunnel_id), self.mid_sigma_min ** 2)
            tunnel.warm_until = self.warm_until_floor
        if not self._sensor(tunnel, d.position, d.sensor_id).accept(d.boot_id, d.seq):
            self.stats.duplicates += 1
            return
        if d.ts > self.clock:
            self.clock = d.ts

        if tunnel.half is None:
            self.stats.calibrating += 1
            half = tunnel.calibrator.observe(self.geo, d.direction, d.lane, d.index, d.ts, d.speed_ms,
                                             d.length_m, d.plate)
            if half is not None:
                tunnel.half = half
                tunnel.calibrator = None
                # Vehicles already inside the tunnel only produce partial events: don't count those misses.
                tunnel.warm_until = max(d.ts + 2 * half / self.min_speed_ms, self.warm_until_floor)
                self.store.dirty = True
                log.info("tunnel %s calibrated: sensor spacing %.1f m (length %.0f m)",
                         tunnel.tunnel_id, half, 2 * half)
                if self.on_calibrated is not None:
                    self.on_calibrated(tunnel.tunnel_id, half)
        else:
            key = (d.direction, d.lane)
            lanes = tunnel.lanes.get(key)
            if lanes is None:
                lanes = tunnel.lanes[key] = ([], [])
            if d.index == 2:
                self._on_last(tunnel, lanes, d)
            else:
                insort(lanes[d.index], d, key=_TS)
                self.stats.pending += 1
                if self.pending_offsets is not None:
                    self.pending_offsets[d.offset] = None
                dt = (2 - d.index) * tunnel.half / d.speed_ms
                var = tunnel.sensors[d.position].speed_var
                if var < self.deadline_var_floor:   # replay: never give up earlier than the previous owner
                    var = self.deadline_var_floor
                deadline = d.ts + dt + self._travel_tol(dt, var, self.search_sigma) + self.lateness
                heapq.heappush(self.expiry, (deadline, next(self.seq), d, tunnel))
        self.advance(self.clock)

    # --- association ------------------------------------------------------

    def _travel_tol(self, dt: float, rel_var: float, sigma: float | None = None) -> float:
        sigma = self.gate_sigma if sigma is None else sigma
        return self.tol_fixed + abs(dt) * max(self.tol_ratio, sigma * min(math.sqrt(rel_var), MAX_SPEED_NOISE))

    def _on_last(self, tunnel: TunnelState, lanes: tuple[list[Detection], list[Detection]], d2: Detection) -> None:
        half, sensors = tunnel.half, tunnel.sensors
        p0, p1 = lanes
        dt = half / d2.speed_ms
        var2 = sensors[d2.position].speed_var
        tol = self._travel_tol(dt, var2, self.search_sigma)
        middles = _window(p1, d2.ts - dt - tol, d2.ts - dt + tol)

        # A plate names the vehicle: a candidate that read a different plate is a different
        # vehicle, however well the timing fits. Candidates without a plate (unreadable) stay,
        # so the timing still does the work wherever the cameras did not.
        middles = _compatible(middles, d2.plate)

        # Triplets: the middle candidate fixes where the first detection must be.
        best, best_score = None, TRIPLET_MAX_SCORE
        # Midpoint gate: learned per tunnel (the middle sensor's jitter dominates), with a floor.
        mid_sigma = max(self.mid_sigma_min, math.sqrt(tunnel.mid_var))
        mid_tol = self.search_sigma * mid_sigma
        for d1 in middles:
            expected0 = 2 * d1.ts - d2.ts
            for d0 in _compatible(_window(p0, expected0 - mid_tol, expected0 + mid_tol), d2.plate):
                span = d2.ts - d0.ts
                if span <= 0:
                    continue
                rt = (d0.ts - expected0) / mid_sigma
                score = rt * rt + self._consistency((d0, d1, d2), 2 * half / span, sensors)
                if score < best_score:
                    best, best_score = (d0, d1, d2), score
        if best is not None:
            self._finalize(tunnel, list(best))
            return

        # Pairs: the other sensor missed this vehicle.
        pair, best_score = None, PAIR_MAX_SCORE
        for d1 in middles:
            score = self._pair_score(d1, d2, half, sensors)
            if score < best_score:
                pair, best_score = [None, d1, d2], score
        dt0 = 2 * dt
        tol0 = self._travel_tol(dt0, var2, self.search_sigma)
        for d0 in _compatible(_window(p0, d2.ts - dt0 - tol0, d2.ts - dt0 + tol0), d2.plate):
            score = self._pair_score(d0, d2, half, sensors)
            if score < best_score:
                pair, best_score = [d0, None, d2], score
        self._finalize(tunnel, pair if pair is not None else [None, None, d2])

    def _consistency(self, dets: tuple[Detection, ...], travel_speed: float, sensors: dict[str, SensorState]) -> float:
        """Chi-square-like distance of measured speeds from the timing speed and of lengths from their mean."""
        mean_len = sum(x.length_m for x in dets) / len(dets)
        score = 0.0
        for x in dets:
            s = sensors[x.position]
            zs = (x.speed_ms - travel_speed) / travel_speed
            zl = x.length_m - mean_len
            score += zs * zs / s.speed_var + zl * zl / s.length_var
        return score

    def _pair_score(self, a: Detection, b: Detection, half: float, sensors: dict[str, SensorState]) -> float:
        """Score of two detections (a before b in travel order) being one vehicle; inf outside the gate."""
        sa, sb = sensors[a.position], sensors[b.position]
        v = (a.speed_ms + b.speed_ms) / 2
        rel_var = sa.speed_var + sb.speed_var
        dt = (b.index - a.index) * half / v
        tol = self._travel_tol(dt, rel_var / 4)
        resid = b.ts - a.ts - dt
        if resid > tol or resid < -tol:
            return math.inf
        rt = resid * self.gate_sigma / tol
        zs = (a.speed_ms - b.speed_ms) / v
        zl = a.length_m - b.length_m
        return rt * rt + zs * zs / rel_var + zl * zl / (sa.length_var + sb.length_var)

    def _expire(self, tunnel: TunnelState, d: Detection, learn: bool = True) -> None:
        """The vehicle of pending detection `d` should have passed the last sensor: that sensor missed it."""
        half, sensors = tunnel.half, tunnel.sensors
        p0, p1 = tunnel.lanes[(d.direction, d.lane)]
        dt = half / d.speed_ms
        tol = self._travel_tol(dt, sensors[d.position].speed_var, self.search_sigma)
        pair, best_score = None, PAIR_MAX_SCORE
        if d.index == 0:
            for d1 in _compatible(_window(p1, d.ts + dt - tol, d.ts + dt + tol), d.plate):
                score = self._pair_score(d, d1, half, sensors)
                if score < best_score:
                    pair, best_score = [d, d1, None], score
        else:
            for d0 in _compatible(_window(p0, d.ts - dt - tol, d.ts - dt + tol), d.plate):
                score = self._pair_score(d0, d, half, sensors)
                if score < best_score:
                    pair, best_score = [d0, d, None], score
        if pair is None:
            pair = [d, None, None] if d.index == 0 else [None, d, None]
        self._finalize(tunnel, pair, learn)

    # --- finalization -----------------------------------------------------

    def advance(self, watermark: float) -> None:
        exp = self.expiry
        while exp and exp[0][0] <= watermark:
            _, _, d, tunnel = heapq.heappop(exp)
            if not d.used:
                self._expire(tunnel, d)

    def tick(self, wall_now: float) -> None:
        """Advance event time on wall time while input is idle, so the last vehicles get emitted."""
        if self.last_input_wall and wall_now - self.last_input_wall > IDLE_ADVANCE_S:
            self.clock += wall_now - self.last_input_wall
            self.last_input_wall = wall_now
            self.advance(self.clock)

    def flush(self) -> None:
        """Emit every pending detection without learning from it (MQTT shutdown)."""
        while self.expiry:
            _, _, d, tunnel = heapq.heappop(self.expiry)
            if not d.used:
                self._expire(tunnel, d, learn=False)

    # --- Kafka: offsets and replay -------------------------------------------

    def watermark(self) -> int:
        """The oldest detection still pending (its vehicle event is not emitted yet), else the next
        offset to consume. Never commit past it. -1 = nothing consumed yet."""
        po = self.pending_offsets
        if po:
            return next(iter(po))
        return self.next_offset

    def replay_start(self, watermark: int | None = None) -> int:
        """Offset to commit, <= watermark(): also covers every detection of the emitted vehicles
        that have a detection at or after the watermark. A new owner reading from here sees
        those vehicles whole instead of as orphan detections that could be fused with the
        vehicles still pending."""
        wm = self.watermark() if watermark is None else watermark
        starts = self.event_starts
        if wm < 0 or not starts:
            return wm
        bucket = wm >> EVENT_BUCKET_BITS
        start = wm
        for b in list(starts):
            if b < bucket:
                del starts[b]           # the watermark never moves back
            elif starts[b] < start:
                start = starts[b]
        return start

    def replay_boundary(self) -> tuple[int, int, float]:
        """(pending watermark, position, event clock in s) to record with a commit: everything
        finalized before `position` has been emitted by this owner (or, while replaying, by the
        previous one), and nothing before the pending watermark was still pending."""
        wm = self.watermark()
        if self.replay_until is not None:
            return (min(wm, self.replay_pending_from), max(self.replay_until, self.next_offset),
                    max(self.replay_clock, self.clock))
        return wm, self.next_offset, self.clock

    def begin_replay(self, until_offset: int, clock_s: float, pending_from: int = 0) -> None:
        """This partition moved here. The records up to `until_offset` were already processed by
        the previous owner, whose event clock was `clock_s`, and none before `pending_from` was
        still pending there. Replay them to rebuild the pending state, but emit nothing
        finalized before `until_offset`: the previous owner emitted those. From `pending_from`
        on, pending detections never expire early during the replay (widest gate), so nothing
        the previous owner still held can be lost."""
        self.replay_until = until_offset
        self.replay_pending_from = pending_from
        self.replay_clock = clock_s
        self.suppress = True
        self.deadline_var_floor = 0.0 if pending_from > max(self.next_offset, 0) else MAX_SPEED_NOISE ** 2
        self.warm_until_floor = clock_s   # vehicles already under way: their misses are not ours to count

    def end_replay(self) -> None:
        """Reached the previous owner's position. A replayed detection that is still pending but
        whose deadline (with the noise learned during the replay) had passed the previous owner's
        clock was already emitted by it: drop it silently. Keep the rest, with normal deadlines."""
        if self.replay_until is None:
            return
        clock = self.replay_clock
        self.replay_until = None
        self.suppress = False
        self.deadline_var_floor = 0.0
        st, po = self.stats, self.pending_offsets
        expiry = []
        seq = self.seq
        for tunnel in self.tunnels.values():
            half = tunnel.half
            if half is None:
                continue
            sensors = tunnel.sensors
            for lanes in tunnel.lanes.values():
                for idx in (0, 1):
                    kept = []
                    for d in lanes[idx]:
                        dt = (2 - idx) * half / d.speed_ms
                        var = sensors[d.position].speed_var
                        deadline = d.ts + dt + self._travel_tol(dt, var, self.search_sigma) + self.lateness
                        safe = d.ts + dt + self._travel_tol(dt, var * REPLAY_NOISE_FACTOR ** 2, self.search_sigma) \
                            + self.lateness + REPLAY_MARGIN_S + REPLAY_MARGIN_RATIO * dt
                        if safe <= clock:
                            d.used = True
                            st.pending -= 1
                            st.replay_dropped += 1
                            if po is not None:
                                po.pop(d.offset, None)
                        else:
                            kept.append(d)
                            expiry.append((deadline, next(seq), d, tunnel))
                    lanes[idx][:] = kept
            # Report windows start now: the replayed part was reported by the previous owner.
            tunnel.reset_window()
            for s in sensors.values():
                s.reset_window()
        heapq.heapify(expiry)
        self.expiry = expiry
        if clock > self.clock:   # the previous owner's clock includes idle time
            self.clock = clock
        self.advance(self.clock)

    def _finalize(self, tunnel: TunnelState, slots: list[Detection | None], learn: bool = True) -> None:
        st = self.stats
        dets = [x for x in slots if x is not None]
        first = dets[0]
        p0, p1 = tunnel.lanes[(first.direction, first.lane)]
        po = self.pending_offsets
        for x in dets:
            x.used = True
            if x.index < 2:
                (p0 if x.index == 0 else p1).remove(x)
                st.pending -= 1
                if po is not None:
                    po.pop(x.offset, None)
        n = len(dets)
        starts = self.event_starts
        if starts is not None:
            lo = hi = first.offset
            for x in dets:
                if x.offset < lo:
                    lo = x.offset
                elif x.offset > hi:
                    hi = x.offset
            b = hi >> EVENT_BUCKET_BITS
            if lo < starts.get(b, hi + 1):
                starts[b] = lo
        suppress = self.suppress
        if suppress:
            st.replay_suppressed += 1
        else:
            st.vehicles += 1
            if n == 3:
                st.vehicles_3 += 1
            elif n == 2:
                st.vehicles_2 += 1
            else:
                st.vehicles_1 += 1

        # --- classification: weighted log-odds vote over the vehicle types ---
        # Each sensor adds log(c (K-1) / (1-c)) to the type it reported; BILINMEYEN ("could not
        # tell") abstains. The winner is the highest total, its confidence the margin to the
        # runner-up, so three sensors that agree are more certain than two out of three.
        scores: dict[str, float] = {}
        weights = self.position_weights
        for x in dets:
            if x.vtype is UNKNOWN or x.vtype == UNKNOWN:
                continue
            c = min(max(x.confidence, self.conf_lo), self.conf_hi)
            w = weights.get(x.position, 1.0)
            scores[x.vtype] = scores.get(x.vtype, 0.0) + w * math.log(c * N_CLASSES_MINUS_1 / (1 - c))
        length = sum(x.length_m for x in dets) / n
        if scores:
            vtype = max(scores, key=scores.__getitem__)
            best = scores[vtype]
            runner_up = max((v for t, v in scores.items() if t != vtype), default=0.0)
            margin = best - runner_up
        else:
            vtype, margin = UNKNOWN, 0.0
        agreement = sum(1 for x in dets if x.vtype == vtype) / n
        confidence = 1 / (1 + math.exp(-min(margin, 30.0))) if n > 1 else min(first.confidence, self.conf_hi)

        half = tunnel.half
        last = dets[-1]
        if n > 1 and last.ts > first.ts:
            speed = (last.index - first.index) * half / (last.ts - first.ts)
        else:
            speed = sum(x.speed_ms for x in dets) / n
        ts_entry = first.ts - first.index * half / speed
        ts_exit = last.ts + (2 - last.index) * half / speed

        if learn:
            self._learn(tunnel, slots, dets, speed)
            if not suppress:
                counts = tunnel.w_counts[first.direction]
                counts[vtype] = counts.get(vtype, 0) + 1
                tunnel.w_speed_sum += speed
                tunnel.w_vehicles += 1
        if suppress:
            return

        event = {
            "schema": SCHEMA_VERSION,
            "event_id": first.message_id,
            "tunnel_id": tunnel.tunnel_id,
            "direction": first.direction,
            "lane": first.lane,
            "ts_entry": int(ts_entry * 1000),
            "ts_exit": int(ts_exit * 1000),
            "plate": _plate_of(dets),
            "speed_kmh": round(speed * 3.6, 1),
            "length_m": round(length, 2),
            "classification": vtype,
            "confidence": round(confidence, 3),
            "agreement": round(agreement, 3),
            "sensors": n,
            "missing": [POSITION_AT[first.direction][i] for i in range(3) if slots[i] is None],
        }
        if self.include_detections:
            event["detections"] = [x.message_id for x in dets]
        if first.true_class is not None or first.vehicle_id is not None:
            self._evaluate(event, dets, vtype)
        self.sink.vehicle(tunnel.tunnel_id, orjson.dumps(event), event, dets)

    def _learn(self, tunnel: TunnelState, slots: list[Detection | None], dets: list[Detection],
               speed: float) -> None:
        n = len(dets)
        if n < 2:
            return
        sensors = tunnel.sensors
        if n == 3:
            # Noise: the timing speed (first/last ts) is independent of every measured speed.
            alpha = self.noise_alpha
            resid = dets[1].ts - (dets[0].ts + dets[2].ts) / 2
            tunnel.mid_var += alpha * (resid * resid - tunnel.mid_var)
            mean_len = sum(x.length_m for x in dets) / 3
            for x in dets:
                sensor = sensors[x.position]
                e = (x.speed_ms - speed) / speed
                sensor.speed_var += alpha * (min(e * e, MAX_SPEED_NOISE ** 2) - sensor.speed_var)
                el = x.length_m - mean_len
                # deviation from a 3-sample mean has 2/3 of the variance
                sensor.length_var += alpha * (min(1.5 * el * el, MAX_LENGTH_NOISE_M ** 2) - sensor.length_var)
            if self.refine_alpha:
                half = tunnel.half
                implied = (dets[2].ts - dets[0].ts) * (dets[0].speed_ms + dets[1].speed_ms + dets[2].speed_ms) / 6
                if abs(implied - half) < REFINE_CLIP * half:
                    tunnel.half = half + self.refine_alpha * (implied - half)
                    self.store.dirty = True

    def _evaluate(self, event: dict, dets: list[Detection], vtype: str) -> None:
        st = self.stats
        st.eval_vehicles += 1
        if len({x.vehicle_id for x in dets}) > 1:
            st.eval_mixed += 1
        truth = dets[0].true_class
        if truth is not None:
            st.eval_correct += truth == vtype
            event["true_class"] = truth
        if dets[0].vehicle_id is not None:
            event["vehicle_id"] = dets[0].vehicle_id

    # --- periodic outputs ---------------------------------------------------

    def report_traffic(self, window_s: float, now_ms: int | None = None) -> None:
        """Publish the per-tunnel traffic window (retained on MQTT), then reset it.

        Per-device health is reported by the devices themselves
        (apps/simulator: tunnels/{tunnel_id}/sensors/{position}/health).
        now_ms: report time (`ts`), default wall clock."""
        ts = int(time.time() * 1000) if now_ms is None else now_ms
        sink = self.sink
        for tunnel in self.tunnels.values():
            tid = tunnel.tunnel_id
            for s in tunnel.sensors.values():
                s.reset_window()
            traffic = {
                "schema": SCHEMA_VERSION,
                "tunnel_id": tid,
                "ts": ts,
                "window_s": round(window_s, 1),
                "length_m": None if tunnel.half is None else round(2 * tunnel.half, 1),
                "vehicles": tunnel.w_vehicles,
                "counts": tunnel.w_counts,
                "avg_speed_kmh": round(tunnel.w_speed_sum / tunnel.w_vehicles * 3.6, 1) if tunnel.w_vehicles else None,
            }
            sink.traffic(tid, orjson.dumps(traffic))
            tunnel.reset_window()

    def learned_halves(self) -> dict[str, float]:
        return {tid: t.half for tid, t in self.tunnels.items() if t.half is not None}

    def save_geometry(self) -> None:
        self.store.save(self.learned_halves())

    def snapshot(self) -> dict:
        st = self.stats
        st.tunnels = len(self.tunnels)
        st.tunnels_calibrated = sum(1 for t in self.tunnels.values() if t.half is not None)
        st.lost = sum(s.lost_total for t in self.tunnels.values() for s in t.sensors.values())
        return asdict(st)
