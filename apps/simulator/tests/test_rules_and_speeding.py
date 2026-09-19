"""Tunnel access rules (banned types per hour) and the handful of daily speeders."""

import random
from collections import Counter
from datetime import datetime

from tunnel_sim.clock import ISTANBUL, SimClock
from tunnel_sim.config import Config
from tunnel_sim.model import VehicleFactory, build_tunnel

MIX = {"OTOMOBIL": 60.0, "KAMYON": 20.0, "CEKICI_YARI_ROMORK": 15.0, "TRAKTOR": 5.0}
PROFILE = {
    "tunnel_id": "TR-TEST", "name": "Test", "city": "Bolu", "city_code": 14,
    "length_m": 2000.0, "lanes_per_direction": 2, "speed_limit_kmh": 80.0,
    "type_mix": MIX,
    "rules": [
        {"types": ["KAMYON", "CEKICI_YARI_ROMORK"], "from": "07:00", "to": "09:00"},
        {"types": ["TRAKTOR"], "from": "22:00", "to": "06:00"},
    ],
}


def at(hour: int, minute: int = 0) -> float:
    return datetime(2026, 6, 15, hour, minute, tzinfo=ISTANBUL).timestamp()


def config(profile_overrides=None, **traffic) -> Config:
    return Config.model_validate({
        "profiles": [{**PROFILE, **(profile_overrides or {})}],
        "sensors": {"miss_rate": 0, "duplicate_rate": 0, "degraded_ratio": 0},
        "traffic": traffic,
    })


def spawn_at(cfg: Config, hour: int, n: int = 4000, seed: str = "rules") -> list:
    clock = SimClock(t0_epoch=at(hour))
    tunnel = build_tunnel(cfg, 0)
    factory = VehicleFactory(cfg, random.Random(seed), clock)
    return [factory.spawn(tunnel, 0.0) for _ in range(n)]


def share(vehicles, vtype: str) -> float:
    return Counter(v.vtype for v in vehicles)[vtype] / len(vehicles)


# --- access rules --------------------------------------------------------

def test_banned_types_almost_disappear_during_their_window():
    allowed = share(spawn_at(config(), hour=12), "KAMYON")
    banned = share(spawn_at(config(), hour=8), "KAMYON")
    assert allowed > 0.15
    assert banned < allowed / 10


def test_a_banned_type_still_shows_up_sometimes_that_is_the_alert():
    banned = spawn_at(config(), hour=8, n=20_000)
    trucks = [v for v in banned if v.vtype in ("KAMYON", "CEKICI_YARI_ROMORK")]
    assert trucks, "violations must still happen, or there is nothing to alert on"
    assert len(trucks) / 20_000 < 0.02


def test_rules_that_wrap_past_midnight_apply_on_both_sides():
    assert share(spawn_at(config(), hour=23), "TRAKTOR") < 0.01
    assert share(spawn_at(config(), hour=3), "TRAKTOR") < 0.01
    assert share(spawn_at(config(), hour=12), "TRAKTOR") > 0.02


def test_unaffected_types_take_over_the_freed_share():
    """Suppressing trucks must not lower the tunnel's message rate — cars fill the gap."""
    assert share(spawn_at(config(), hour=8), "OTOMOBIL") > share(spawn_at(config(), hour=12), "OTOMOBIL")


def test_violation_factor_is_configurable():
    strict = config(violation_factor=0.0)
    assert share(spawn_at(strict, hour=8), "KAMYON") == 0.0


def test_an_all_day_rule_suppresses_the_type_around_the_clock():
    cfg = config({"rules": [{"types": ["KAMYON"], "from": "00:00", "to": "24:00"}]})
    assert all(share(spawn_at(cfg, hour=h), "KAMYON") < 0.01 for h in (0, 8, 14, 23))


# --- speeding ------------------------------------------------------------

def days_of_speeders(cfg: Config, days: int = 20) -> list[int]:
    """Speeders per simulated day, at the tunnel's own arrival rate."""
    tunnel = build_tunnel(cfg, 0)
    clock = SimClock(t0_epoch=at(0))
    factory = VehicleFactory(cfg, random.Random("speed"), clock)
    per_day = []
    for day in range(days):
        t = day * 86400.0
        end = t + 86400.0
        speeders = 0
        while t < end:
            t += factory.next_arrival_gap(tunnel, t)
            speeders += factory.spawn(tunnel, t).speeding
        per_day.append(speeders)
    return per_day


def test_a_couple_of_vehicles_exceed_the_limit_every_day():
    per_day = days_of_speeders(config())
    mean = sum(per_day) / len(per_day)
    assert 1.5 < mean < 3.5
    assert len(set(per_day)) > 1, "there should be a bit of randomness, not exactly n every day"


def test_a_speeder_actually_exceeds_the_tunnel_limit():
    # A rate high enough to get a sample; how often speeders happen is the test above.
    cfg = config(speeding_per_day=[20_000.0, 20_000.0])
    tunnel = build_tunnel(cfg, 0)
    factory = VehicleFactory(cfg, random.Random("speed"), SimClock(t0_epoch=at(12)))
    vehicles = [factory.spawn(tunnel, float(i)) for i in range(20_000)]
    speeders = [v for v in vehicles if v.speeding]
    assert speeders
    assert all(v.speed_ms * 3.6 > tunnel.profile.speed_limit_kmh for v in speeders)
    assert max(v.speed_ms * 3.6 for v in speeders) < tunnel.profile.speed_limit_kmh * 1.6


def test_normal_traffic_stays_close_to_the_limit():
    tunnel = build_tunnel(config(), 0)
    factory = VehicleFactory(config(), random.Random("normal"), SimClock(t0_epoch=at(12)))
    normal = [v for v in (factory.spawn(tunnel, float(i)) for i in range(5000)) if not v.speeding]
    over = sum(v.speed_ms * 3.6 > tunnel.profile.speed_limit_kmh for v in normal) / len(normal)
    assert over < 0.02
    mean_kmh = sum(v.speed_ms for v in normal) / len(normal) * 3.6
    assert 55 < mean_kmh < 80


def test_the_speeding_rate_is_configurable():
    per_day = days_of_speeders(config(speeding_per_day=[10.0, 12.0]))
    assert 8 < sum(per_day) / len(per_day) < 14
