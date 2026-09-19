"""The 10 Turkish vehicle types: spawn mix, per-type physics, and how a sensor classifies them."""

import random
from collections import Counter

from tunnel_sim.config import Config
from tunnel_sim.model import VehicleFactory, build_tunnel, detect
from tunnel_sim.vehicles import PHYSICAL, SIZE_ORDER, SPAWNABLE_TYPES, UNKNOWN, VEHICLE_TYPES

PROFILE = {
    "tunnel_id": "TR-TEST",
    "name": "Test Tüneli",
    "city": "Bolu",
    "city_code": 14,
    "length_m": 2000.0,
    "lanes_per_direction": 2,
    "speed_limit_kmh": 80.0,
    "type_mix": {"OTOMOBIL": 70.0, "KAMYON": 30.0},
}
NOISELESS = dict(length_noise_std_m=0, misclassification_rate=0, unknown_rate=0, miss_rate=0,
                 duplicate_rate=0, timestamp_jitter_ms=0, speed_noise_ratio=0, degraded_ratio=0)


def config(profile_overrides=None, **sensors) -> Config:
    return Config.model_validate({
        "profiles": [{**PROFILE, **(profile_overrides or {})}],
        "sensors": {**NOISELESS, **sensors},
    })


def spawn_many(cfg: Config, n: int) -> list:
    tunnel = build_tunnel(cfg, 0)
    factory = VehicleFactory(cfg, random.Random("spawn"))
    return [factory.spawn(tunnel, t_entry=float(i)) for i in range(n)]


# --- types ---------------------------------------------------------------

def test_the_ten_turkish_types_are_the_vehicle_types():
    assert set(VEHICLE_TYPES) == {
        "MOTOSIKLET", "OTOMOBIL", "HAFIF_TICARI", "MINIBUS", "OTOBUS", "KAMYON",
        "CEKICI_YARI_ROMORK", "TRAKTOR", "OZEL_AMACLI_TASIT", "BILINMEYEN"}
    assert UNKNOWN not in SPAWNABLE_TYPES
    assert set(SIZE_ORDER) == set(SPAWNABLE_TYPES)
    assert [PHYSICAL[t].length_m.mean for t in SIZE_ORDER] == sorted(PHYSICAL[t].length_m.mean for t in SIZE_ORDER)


# --- spawning ------------------------------------------------------------

def test_tunnel_takes_geometry_city_and_speed_limit_from_its_profile():
    tunnel = build_tunnel(config(), 0)
    assert tunnel.tunnel_id == "TR-TEST"
    assert tunnel.length_m == 2000.0
    assert tunnel.lanes_per_direction == 2
    assert tunnel.profile.speed_limit_kmh == 80.0
    assert (tunnel.profile.city, tunnel.profile.city_code) == ("Bolu", 14)
    assert [s.sensor_id for s in tunnel.sensors] == ["TR-TEST-start", "TR-TEST-middle", "TR-TEST-end"]


def test_spawned_types_follow_the_tunnels_type_mix():
    counts = Counter(v.vtype for v in spawn_many(config(), 4000))
    assert set(counts) == {"OTOMOBIL", "KAMYON"}          # zero-weight types never spawn
    assert 0.65 < counts["OTOMOBIL"] / 4000 < 0.75


def test_a_different_tunnel_mix_gives_a_different_composition():
    freight = config({"type_mix": {"CEKICI_YARI_ROMORK": 80.0, "OTOMOBIL": 20.0}})
    counts = Counter(v.vtype for v in spawn_many(freight, 2000))
    assert counts["CEKICI_YARI_ROMORK"] / 2000 > 0.7


def test_type_determines_length_and_speed():
    cfg = config({"type_mix": {t: 1.0 for t in SPAWNABLE_TYPES}})
    by_type = {}
    for v in spawn_many(cfg, 6000):
        by_type.setdefault(v.vtype, []).append(v)
    mean_len = {t: sum(v.length_m for v in vs) / len(vs) for t, vs in by_type.items()}
    assert mean_len["MOTOSIKLET"] < mean_len["OTOMOBIL"] < mean_len["KAMYON"] < mean_len["CEKICI_YARI_ROMORK"]
    # speeds stay near the 80 km/h limit, and the tractor is capped well below it
    mean_kmh = {t: sum(v.speed_ms for v in vs) / len(vs) * 3.6 for t, vs in by_type.items()}
    assert 60 < mean_kmh["OTOMOBIL"] < 90
    assert mean_kmh["TRAKTOR"] < 50
    assert all(v.speed_ms > 0 for vs in by_type.values() for v in vs)


# --- classification ------------------------------------------------------

def test_every_sensor_of_a_tunnel_reports_the_true_type_when_noiseless():
    cfg = config({"type_mix": {t: 1.0 for t in SPAWNABLE_TYPES}})
    tunnel = build_tunnel(cfg, 0)
    factory = VehicleFactory(cfg, random.Random("classify"))
    rng = random.Random("detect")
    for i in range(500):
        v = factory.spawn(tunnel, float(i))
        reported = [detect(s, v, rng).classification for s in v.order]
        assert reported == [v.vtype, v.vtype, v.vtype]


def test_misclassification_reports_a_neighbouring_size_class():
    cfg = config({"type_mix": {t: 1.0 for t in SPAWNABLE_TYPES}}, misclassification_rate=1.0)
    tunnel = build_tunnel(cfg, 0)
    factory = VehicleFactory(cfg, random.Random("classify"))
    rng = random.Random("detect")
    index = {t: i for i, t in enumerate(SIZE_ORDER)}
    for i in range(500):
        v = factory.spawn(tunnel, float(i))
        d = detect(v.order[0], v, rng)
        assert d.classification != v.vtype
        assert abs(index[d.classification] - index[v.vtype]) == 1
        assert d.confidence < 0.8


def test_sensor_reports_bilinmeyen_with_low_confidence_when_it_cannot_decide():
    cfg = config(unknown_rate=1.0)
    tunnel = build_tunnel(cfg, 0)
    factory = VehicleFactory(cfg, random.Random("classify"))
    rng = random.Random("detect")
    for i in range(100):
        v = factory.spawn(tunnel, float(i))
        d = detect(v.order[0], v, rng)
        assert d.classification == UNKNOWN
        assert d.confidence <= 0.5


def test_confidence_is_high_for_a_confident_correct_reading():
    cfg = config()
    tunnel = build_tunnel(cfg, 0)
    factory = VehicleFactory(cfg, random.Random("classify"))
    rng = random.Random("detect")
    v = factory.spawn(tunnel, 0.0)
    assert detect(v.order[0], v, rng).confidence > 0.8


def test_more_types_do_not_change_the_message_rate():
    """One vehicle is still one detection per sensor, whatever the mix."""
    rng = random.Random("rate")
    counts = []
    for mix in ({"OTOMOBIL": 1.0}, {t: 1.0 for t in SPAWNABLE_TYPES}):
        cfg = config({"type_mix": mix})
        tunnel = build_tunnel(cfg, 0)
        factory = VehicleFactory(cfg, random.Random("rate"))
        detections = 0
        for i in range(200):
            v = factory.spawn(tunnel, float(i))
            detections += sum(detect(s, v, rng) is not None for s in v.order)
        counts.append(detections)
    assert counts[0] == counts[1] == 600


# --- generator mode ------------------------------------------------------

def test_generator_mode_synthesizes_a_profile_deterministically():
    cfg = Config.model_validate({"topology": {"mode": "generator", "tunnels": 50}, "sensors": NOISELESS})
    a, b = build_tunnel(cfg, 7), build_tunnel(cfg, 7)
    assert a.tunnel_id == b.tunnel_id == "T000007"
    assert a.profile.speed_limit_kmh == b.profile.speed_limit_kmh > 0
    assert a.profile.city == b.profile.city and 1 <= a.profile.city_code <= 81
    assert a.profile.type_mix == b.profile.type_mix and a.profile.type_mix
    assert build_tunnel(cfg, 8).profile.type_mix  # every generated tunnel has a usable mix
