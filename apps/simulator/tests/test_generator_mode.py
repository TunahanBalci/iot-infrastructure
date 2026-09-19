"""Generator mode: the load-test path. Same schema, cheapest possible generation."""

import random

import pytest

from tunnel_sim.clock import ISTANBUL, SimClock
from tunnel_sim.config import Config, load_config
from tunnel_sim.model import VehicleFactory, build_tunnel

from datetime import datetime


def generator(**topology) -> Config:
    return Config.model_validate({"topology": {"mode": "generator", "tunnels": 1000, **topology}})


def at(hour: int) -> float:
    return datetime(2026, 6, 15, hour, tzinfo=ISTANBUL).timestamp()


# --- target load ---------------------------------------------------------

def test_a_target_load_sets_the_per_device_rate():
    cfg = Config.model_validate({"topology": {"mode": "generator", "tunnels": 1000},
                                 "traffic": {"target_total_msgs_s": 50_000}})
    assert cfg.traffic.msgs_per_device_s == pytest.approx(50_000 / 3000)
    assert cfg.expected_msgs_per_s() == pytest.approx(50_000, rel=0.05)


def test_the_target_load_reaches_the_workers_through_the_environment():
    cfg = load_config("config/simulator.yaml", environ={"SIM__TOPOLOGY__MODE": "generator",
                                                        "SIM__TOPOLOGY__TUNNELS": "2000",
                                                        "SIM__TRAFFIC__TARGET_TOTAL_MSGS_S": "120000"})
    assert cfg.traffic.msgs_per_device_s == pytest.approx(120_000 / 6000)


def test_without_a_target_the_configured_rate_stands():
    assert generator().traffic.msgs_per_device_s == 2.0


def test_a_target_load_in_profile_mode_spreads_over_the_profile_tunnels():
    cfg = load_config("config/simulator.yaml", environ={"SIM__TRAFFIC__TARGET_TOTAL_MSGS_S": "150"})
    assert cfg.tunnel_count() == 5
    assert cfg.traffic.msgs_per_device_s == pytest.approx(10.0)


# --- cheap generation ----------------------------------------------------

def test_generator_traffic_has_no_daily_curve():
    """A load test must hold a steady rate; the evening burst belongs to the profile tunnels."""
    cfg = generator()
    factory = VehicleFactory(cfg, random.Random("gen"), SimClock(t0_epoch=at(18)))
    tunnel = build_tunnel(cfg, 0)
    assert factory.rate(tunnel, 0.0) == pytest.approx(cfg.traffic.msgs_per_device_s * tunnel.rate_multiplier)


def test_profile_traffic_keeps_its_daily_curve():
    cfg = load_config("config/simulator.yaml")
    factory = VehicleFactory(cfg, random.Random("prof"), SimClock(t0_epoch=at(18)))
    tunnel = build_tunnel(cfg, 0)
    assert factory.rate(tunnel, 0.0) > cfg.traffic.msgs_per_device_s


def test_generator_tunnels_have_no_rules_to_evaluate():
    cfg = generator()
    assert all(build_tunnel(cfg, i).profile.rules == [] for i in range(5))


def test_generator_payloads_carry_the_same_fields_as_profile_payloads():
    import orjson

    from tunnel_sim.model import detect
    from tunnel_sim.payload import PayloadEncoder

    def one(cfg: Config) -> set[str]:
        tunnel = build_tunnel(cfg, 0)
        v = VehicleFactory(cfg, random.Random("x"), SimClock(0.0)).spawn(tunnel, 0.0)
        d = detect(v.order[0], v, random.Random("y"))
        return set(orjson.loads(PayloadEncoder(cfg.payload, "b00t").encode(d)))

    assert one(generator()) == one(load_config("config/simulator.yaml"))


def test_generator_vehicles_still_get_plates_and_types():
    cfg = generator()
    tunnel = build_tunnel(cfg, 3)
    factory = VehicleFactory(cfg, random.Random("gen"), SimClock(0.0))
    vehicles = [factory.spawn(tunnel, float(i)) for i in range(200)]
    assert all(v.plate and v.vtype for v in vehicles)
    assert len({v.plate for v in vehicles}) == 200
