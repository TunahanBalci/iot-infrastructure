"""Tunnel profiles: the curated tunnels.yaml, its validation, and how the topology uses it."""

import pytest

from tunnel_sim.config import load_config
from tunnel_sim.profiles import TimeWindow, TunnelProfile, load_profiles
from tunnel_sim.vehicles import VEHICLE_TYPES

MINIMAL = {
    "tunnel_id": "TR-TEST",
    "name": "Test Tüneli",
    "city": "Bolu",
    "city_code": 14,
    "length_m": 3300.0,
    "lanes_per_direction": 2,
    "speed_limit_kmh": 80.0,
    "type_mix": {"OTOMOBIL": 70.0, "KAMYON": 30.0},
}


def profile(**overrides) -> dict:
    return {**MINIMAL, **overrides}


# --- the curated file ----------------------------------------------------

def test_curated_tunnels_yaml_loads_five_tunnels_with_distinct_ids():
    profiles = load_profiles("config/tunnels.yaml")
    assert len(profiles) == 5
    assert len({p.tunnel_id for p in profiles}) == 5
    avrasya = next(p for p in profiles if p.tunnel_id == "TR-AVRASYA")
    assert (avrasya.city, avrasya.city_code, avrasya.speed_limit_kmh) == ("İstanbul", 34, 70.0)


def test_every_curated_tunnel_has_a_type_mix_and_at_least_one_rule_exists():
    profiles = load_profiles("config/tunnels.yaml")
    assert all(p.type_mix for p in profiles)
    assert sum(len(p.rules) for p in profiles) > 0


# --- validation ----------------------------------------------------------

def test_unknown_vehicle_type_in_type_mix_is_rejected():
    with pytest.raises(ValueError, match="SEDAN"):
        TunnelProfile.model_validate(profile(type_mix={"SEDAN": 1.0}))


def test_unknown_vehicle_type_in_a_rule_is_rejected():
    with pytest.raises(ValueError, match="LORRY"):
        TunnelProfile.model_validate(profile(rules=[{"types": ["LORRY"], "from": "07:00", "to": "09:00"}]))


def test_bilinmeyen_is_a_sensor_output_not_a_vehicle_that_can_be_spawned():
    assert "BILINMEYEN" in VEHICLE_TYPES
    with pytest.raises(ValueError, match="BILINMEYEN"):
        TunnelProfile.model_validate(profile(type_mix={"OTOMOBIL": 1.0, "BILINMEYEN": 1.0}))


def test_type_mix_weights_must_be_positive_and_not_all_zero():
    with pytest.raises(ValueError):
        TunnelProfile.model_validate(profile(type_mix={"OTOMOBIL": -1.0}))
    with pytest.raises(ValueError):
        TunnelProfile.model_validate(profile(type_mix={"OTOMOBIL": 0.0, "KAMYON": 0.0}))


def test_city_code_must_be_a_turkish_province_code():
    TunnelProfile.model_validate(profile(city_code=81))
    for bad in (0, 82):
        with pytest.raises(ValueError):
            TunnelProfile.model_validate(profile(city_code=bad))


def test_duplicate_tunnel_ids_are_rejected(tmp_path):
    path = tmp_path / "tunnels.yaml"
    entry = ("  - {tunnel_id: TR-SAME, name: X, city: Bolu, city_code: 14, length_m: 500,\n"
             "      lanes_per_direction: 1, speed_limit_kmh: 80, type_mix: {OTOMOBIL: 1}}\n")
    path.write_text("tunnels:\n" + entry * 2)
    with pytest.raises(ValueError, match="TR-SAME"):
        load_profiles(path)


def test_empty_profile_file_is_rejected(tmp_path):
    path = tmp_path / "tunnels.yaml"
    path.write_text("tunnels: []\n")
    with pytest.raises(ValueError):
        load_profiles(path)


# --- time windows --------------------------------------------------------

def test_time_window_covers_the_hours_between_from_and_to():
    w = TimeWindow.parse("07:00", "09:00")
    assert w.contains(7, 0) and w.contains(8, 59)
    assert not w.contains(9, 0) and not w.contains(6, 59) and not w.contains(20, 0)


def test_time_window_wraps_past_midnight():
    w = TimeWindow.parse("22:00", "06:00")
    assert w.contains(22, 0) and w.contains(23, 30) and w.contains(0, 0) and w.contains(5, 59)
    assert not w.contains(6, 0) and not w.contains(12, 0)


def test_all_day_window_contains_every_hour():
    w = TimeWindow.parse("00:00", "24:00")
    assert all(w.contains(h, 0) for h in range(24))


def test_malformed_time_is_rejected():
    for bad in ("7", "25:00", "07:60", "seven"):
        with pytest.raises(ValueError):
            TimeWindow.parse(bad, "09:00")


# --- topology ------------------------------------------------------------

def test_profile_mode_is_the_default_and_takes_its_tunnels_from_the_yaml():
    cfg = load_config("config/simulator.yaml")
    assert cfg.topology.mode == "profiles"
    assert [p.tunnel_id for p in cfg.profiles] == [p.tunnel_id for p in load_profiles("config/tunnels.yaml")]
    assert cfg.tunnel_count() == 5


def test_generator_mode_ignores_the_profiles_and_uses_the_generated_topology():
    cfg = load_config("config/simulator.yaml", environ={"SIM__TOPOLOGY__MODE": "generator",
                                                        "SIM__TOPOLOGY__TUNNELS": "1000"})
    assert cfg.profiles == []
    assert cfg.tunnel_count() == 1000
