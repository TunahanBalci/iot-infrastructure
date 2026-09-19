"""Curated tunnel profiles: identity, geometry, speed limit, traffic mix and access rules.

`config/tunnels.yaml` is the single source of truth: the simulator reads it, and the install
renders it into ClickHouse (deploy/storage/schema/50-profiles.sql) so the alert queries can
join detections against the same limits and rules.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator

from .vehicles import SPAWNABLE_TYPES, UNKNOWN, VEHICLE_TYPES

_TIME = re.compile(r"^([01]\d|2[0-4]):([0-5]\d)$")


def _minutes(value: str) -> int:
    m = _TIME.match(value) if isinstance(value, str) else None
    if m is None:
        raise ValueError(f"time must be HH:MM (00:00..24:00), got {value!r}")
    return int(m.group(1)) * 60 + int(m.group(2))


class TimeWindow:
    """Local-time window [start, end). Wraps past midnight when end <= start."""

    __slots__ = ("start", "end")

    def __init__(self, start: int, end: int):
        self.start = start
        self.end = end

    @classmethod
    def parse(cls, start: str, end: str) -> TimeWindow:
        return cls(_minutes(start), _minutes(end))

    @property
    def duration_minutes(self) -> int:
        return (self.end - self.start) % 1440 or (1440 if self.end != self.start else 0)

    def contains(self, hour: int, minute: int = 0) -> bool:
        t = hour * 60 + minute
        if self.end > self.start:
            return self.start <= t < self.end
        return t >= self.start or t < self.end  # wraps midnight

    def __repr__(self) -> str:
        return f"TimeWindow({self.start // 60:02d}:{self.start % 60:02d}-{self.end // 60:02d}:{self.end % 60:02d})"


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _known_types(values: list[str]) -> list[str]:
    unknown = [v for v in values if v not in VEHICLE_TYPES]
    if unknown:
        raise ValueError(f"unknown vehicle type(s): {', '.join(unknown)}")
    return values


class Rule(_Model):
    """Vehicle types that may not use the tunnel during a window of the day."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    types: list[str] = Field(min_length=1)
    start: str = Field(alias="from")
    end: str = Field(alias="to")
    _win: TimeWindow = PrivateAttr()

    check_types = field_validator("types")(_known_types)

    @model_validator(mode="after")
    def _parse_window(self) -> Rule:
        self._win = TimeWindow.parse(self.start, self.end)
        return self

    @property
    def window(self) -> TimeWindow:
        return self._win

    def forbids(self, vehicle_type: str, hour: int, minute: int = 0) -> bool:
        return vehicle_type in self.types and self.window.contains(hour, minute)


class TunnelProfile(_Model):
    tunnel_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    city: str = Field(min_length=1)
    city_code: int = Field(ge=1, le=81)          # Turkish province code, also the plate prefix
    length_m: float = Field(gt=0)
    lanes_per_direction: int = Field(ge=1)
    speed_limit_kmh: float = Field(gt=0)
    type_mix: dict[str, float]                   # relative spawn weights
    rules: list[Rule] = Field(default_factory=list)

    @field_validator("type_mix")
    @classmethod
    def _mix(cls, mix: dict[str, float]) -> dict[str, float]:
        _known_types(list(mix))
        if UNKNOWN in mix:
            raise ValueError(f"{UNKNOWN} is a sensor output, not a vehicle that can be spawned")
        if any(w < 0 for w in mix.values()):
            raise ValueError("type_mix weights must be >= 0")
        if sum(mix.values()) <= 0:
            raise ValueError("type_mix must have at least one positive weight")
        return mix

    def forbidden_types(self, hour: int, minute: int = 0) -> frozenset[str]:
        return frozenset(t for r in self.rules for t in r.types if r.window.contains(hour, minute))


def load_profiles(path: str | Path) -> list[TunnelProfile]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"tunnel profiles not found: {path}")
    with path.open() as f:
        data: Any = yaml.safe_load(f) or {}
    if not isinstance(data, dict) or "tunnels" not in data:
        raise ValueError(f"{path}: expected a top-level 'tunnels:' list")
    profiles = [TunnelProfile.model_validate(t) for t in data["tunnels"]]
    if not profiles:
        raise ValueError(f"{path}: no tunnels defined")
    seen: set[str] = set()
    for p in profiles:
        if p.tunnel_id in seen:
            raise ValueError(f"{path}: duplicate tunnel_id {p.tunnel_id}")
        seen.add(p.tunnel_id)
    return profiles


__all__ = ["Rule", "TimeWindow", "TunnelProfile", "load_profiles", "SPAWNABLE_TYPES"]
