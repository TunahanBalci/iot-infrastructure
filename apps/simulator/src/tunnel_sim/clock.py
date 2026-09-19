"""Simulated wall clock.

Simulation time is seconds relative to the shared epoch T0. The clock turns that into a real
instant: `T0 + sim_t + offset`, rendered in Europe/Istanbul — the time of day that drives the
evening burst, the tunnels' access rules and the "daily" dashboards.

T0 is the host clock when the container started, so the simulation starts in sync with the
host. `set_offset` / `set_local` move the simulated time of day; `resync` puts it back on the
host clock. The offset lives in memory only (shared with the workers through a
multiprocessing Value), so a restart always comes back synced.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

# Türkiye has been on permanent UTC+3 since 2016 — no DST transitions to model.
ISTANBUL = timezone(timedelta(hours=3), "Europe/Istanbul")

SECONDS_PER_DAY = 86400


class SimClock:
    """Shared between the supervisor (which sets the offset) and the workers (which read it)."""

    __slots__ = ("t0_epoch", "_offset")

    def __init__(self, t0_epoch: float, offset_value=None):
        self.t0_epoch = t0_epoch
        self._offset = offset_value  # multiprocessing.Value('d'), or None for a local clock

    @property
    def offset(self) -> float:
        return 0.0 if self._offset is None else self._offset.value

    def set_offset(self, seconds: float) -> None:
        if self._offset is None:
            self._offset = _Local(seconds)
        else:
            self._offset.value = seconds

    def resync(self) -> None:
        """Back to host time."""
        self.set_offset(0.0)

    def set_local(self, when: datetime) -> None:
        """Jump so that simulation time 0 reads `when` (naive datetimes are Istanbul local)."""
        if when.tzinfo is None:
            when = when.replace(tzinfo=ISTANBUL)
        self.set_offset(when.timestamp() - self.t0_epoch)

    def epoch(self, sim_t: float) -> float:
        return self.t0_epoch + sim_t + self.offset

    def local(self, sim_t: float) -> datetime:
        return datetime.fromtimestamp(self.epoch(sim_t), ISTANBUL)

    def minute_of_day(self, sim_t: float) -> int:
        """Local minutes since midnight — what the burst window and the tunnel rules compare."""
        local_s = (self.epoch(sim_t) + ISTANBUL.utcoffset(None).total_seconds()) % SECONDS_PER_DAY
        return int(local_s // 60)


class _Local:
    """Stand-in for a multiprocessing Value when the clock is not shared (tests, single process)."""

    __slots__ = ("value",)

    def __init__(self, value: float = 0.0):
        self.value = value
