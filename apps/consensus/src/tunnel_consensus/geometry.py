"""Per-tunnel sensor spacing, learned from traffic.

Sensors sit at start, middle and end, so adjacent sensors are L/2 apart. A vehicle
seen by adjacent sensors at t1 and t2 with speed v implies L/2 = (t2 - t1) * v.
Every pair of plausible detections casts a vote in a log-scale histogram of implied
L/2. Pairs of the same vehicle agree to within the speed noise and pile up in one
bin; unrelated pairs spread thinly. Votes are weighted by how similar the two
measured speeds and lengths are, which suppresses unrelated pairs further. When the
devices read plates, a pair that reads the same plate is the same vehicle and votes
at full weight — traffic that all moves at the posted limit is otherwise hard to tell
apart by timing alone.
"""

from __future__ import annotations

import json
import logging
import math
import os
from collections import deque
from pathlib import Path

from .config import GeometryConfig

log = logging.getLogger(__name__)

SPEED_SIMILARITY_RATIO = 0.06   # std of the speed difference of one vehicle, relative
LENGTH_SIMILARITY_M = 1.5       # std of the length difference of one vehicle
MIN_VOTE_WEIGHT = 0.01
CHECK_EVERY_VOTES = 2.0
PEAK_HALF_WIDTH = 0.03          # relative half width of a peak in the histogram


class GeometryParams:
    """Derived constants shared by all calibrators of a partition."""

    def __init__(self, cfg: GeometryConfig):
        self.min_half = cfg.length_m[0] / 2
        self.max_half = cfg.length_m[1] / 2
        self.window_s = self.max_half / (cfg.speed_kmh[0] / 3.6)
        self.min_length = cfg.calibration_min_length_m
        self.log_step = math.log1p(cfg.bin_ratio)
        # One vehicle's votes spread with the speed noise of two sensors (~2-3%).
        self.peak_bins = max(1, math.ceil(PEAK_HALF_WIDTH / cfg.bin_ratio))
        self.min_votes = cfg.min_votes
        self.peak_ratio = cfg.peak_ratio


class Calibrator:
    __slots__ = ("anchors", "votes", "sums", "since_check")

    def __init__(self) -> None:
        # (direction, lane, travel index) -> recent detections (ts, speed_ms, length_m, plate)
        self.anchors: dict[tuple[str, int, int], deque[tuple[float, float, float, str]]] = {}
        self.votes: dict[int, float] = {}
        self.sums: dict[int, float] = {}
        self.since_check = 0.0

    def observe(self, p: GeometryParams, direction: str, lane: int, index: int,
                ts: float, speed_ms: float, length_m: float, plate: str = "") -> float | None:
        """Feed one detection; returns the learned half length once it is unambiguous."""
        if length_m < p.min_length and not plate:
            return None
        if index > 0:
            prev = self.anchors.get((direction, lane, index - 1))
            if prev:
                while prev and ts - prev[0][0] > p.window_s:
                    prev.popleft()
                exp, log_step, votes, sums = math.exp, p.log_step, self.votes, self.sums
                for a_ts, a_speed, a_len, a_plate in prev:
                    dt = ts - a_ts
                    if dt <= 0:
                        continue
                    same_plate = bool(plate) and a_plate == plate
                    if plate and a_plate and not same_plate:
                        continue          # two different vehicles: nothing to learn from the pair
                    v = (a_speed + speed_ms) / 2
                    half = dt * v
                    if half < p.min_half or half > p.max_half:
                        continue
                    if same_plate:
                        w = 1.0           # certainly one vehicle passing two sensors
                    else:
                        if length_m < p.min_length:
                            continue      # only long vehicles vote when there is no plate to match
                        ds = (a_speed - speed_ms) / (SPEED_SIMILARITY_RATIO * v)
                        dl = (a_len - length_m) / LENGTH_SIMILARITY_M
                        w = exp(-0.5 * (ds * ds + dl * dl))
                        if w < MIN_VOTE_WEIGHT:
                            continue
                    b = int(math.log(half) / log_step)
                    votes[b] = votes.get(b, 0.0) + w
                    sums[b] = sums.get(b, 0.0) + w * half
                    self.since_check += w
        if index < 2 and (plate or length_m >= p.min_length):
            key = (direction, lane, index)
            q = self.anchors.get(key)
            if q is None:
                q = self.anchors[key] = deque()
            while q and ts - q[0][0] > p.window_s:
                q.popleft()
            q.append((ts, speed_ms, length_m, plate))
        if self.since_check >= CHECK_EVERY_VOTES:
            self.since_check = 0.0
            return self.decide(p)
        return None

    def decide(self, p: GeometryParams) -> float | None:
        votes, r = self.votes, p.peak_bins
        if not votes:
            return None
        peaks = {b: sum(votes.get(i, 0.0) for i in range(b - r, b + r + 1)) for b in votes}
        best = max(peaks, key=peaks.__getitem__)
        if peaks[best] < p.min_votes:
            return None
        # A rival must be a separate peak, not the shoulder of the winning one.
        rival = max((v for b, v in peaks.items() if abs(b - best) > 3 * r), default=0.0)
        if peaks[best] < p.peak_ratio * rival:
            return None
        bins = range(best - r, best + r + 1)
        return sum(self.sums.get(b, 0.0) for b in bins) / sum(votes.get(b, 0.0) for b in bins)


class GeometryStore:
    """Learned half lengths, persisted as JSON so restarts skip calibration."""

    def __init__(self, state_dir: str | None, partition: int):
        self.path = Path(state_dir) / f"geometry-p{partition}.json" if state_dir else None
        self.dirty = False

    def load(self) -> dict[str, float]:
        """Loads every partition's file: tunnel ownership may have moved since the last run."""
        if self.path is None or not self.path.parent.is_dir():
            return {}
        halves: dict[str, float] = {}
        for f in sorted(self.path.parent.glob("geometry-p*.json")):
            try:
                data = json.loads(f.read_text())
                halves.update({k: float(v) for k, v in data.get("half_length_m", {}).items()})
            except (OSError, ValueError, AttributeError) as e:
                log.warning("ignoring unreadable geometry state %s: %s", f, e)
        return halves

    def save(self, halves: dict[str, float]) -> None:
        if self.path is None or not self.dirty:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"half_length_m": {k: round(v, 2) for k, v in halves.items()}}))
        os.replace(tmp, self.path)
        self.dirty = False
