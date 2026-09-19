"""Turkish plate numbers: legal format, uniqueness, and the province mix of a tunnel."""

import random
import re
from collections import Counter

from tunnel_sim.config import Config
from tunnel_sim.model import VehicleFactory, build_tunnel
from tunnel_sim.plates import CAPACITY, PlateMinter, encode

# "34 A 1234" | "34 AB 1234" | "34 ABC 12"
PLATE = re.compile(r"^(0[1-9]|[1-7]\d|8[01]) (?:[A-Z] \d{4}|[A-Z]{2} \d{4}|[A-Z]{3} \d{2})$")

PROFILE = {
    "tunnel_id": "TR-TEST", "name": "Test", "city": "Bolu", "city_code": 14,
    "length_m": 2000.0, "lanes_per_direction": 2, "speed_limit_kmh": 80.0,
    "type_mix": {"OTOMOBIL": 1.0},
}


def config(tunnels: list[dict] | None = None) -> Config:
    return Config.model_validate({"profiles": tunnels or [PROFILE],
                                  "sensors": {"miss_rate": 0, "duplicate_rate": 0}})


# --- encoding ------------------------------------------------------------

def test_every_code_of_the_space_is_a_legal_plate_body():
    for i in (0, 1, 9999, 260_000, 260_001, 7_019_999, 7_020_000, CAPACITY - 1):
        assert re.fullmatch(r"[A-Z] \d{4}|[A-Z]{2} \d{4}|[A-Z]{3} \d{2}", encode(i)), encode(i)


def test_encoding_is_injective_over_the_whole_space():
    sample = list(range(0, CAPACITY, 9973))
    assert len({encode(i) for i in sample}) == len(sample)


# --- minting -------------------------------------------------------------

def test_minted_plates_have_the_turkish_format():
    minter = PlateMinter(tunnel_index=0, tunnel_count=5, city_code=14, seed=42)
    for seq in range(1, 2000):
        assert PLATE.match(minter.mint(seq)), minter.mint(seq)


def test_plates_are_unique_across_tunnels_and_vehicles():
    minters = [PlateMinter(tunnel_index=i, tunnel_count=5, city_code=14 + i, seed=42) for i in range(5)]
    plates = [m.mint(seq) for m in minters for seq in range(1, 40_000)]
    assert len(set(plates)) == len(plates)


def test_the_province_varies_between_consecutive_vehicles():
    """Traffic is not one province after another in blocks; the code is what makes plates unique."""
    minter = PlateMinter(tunnel_index=0, tunnel_count=1, city_code=14, seed=42)
    provinces = [minter.mint(seq)[:2] for seq in range(1, 60)]
    assert len(set(provinces)) > 3
    assert any(a != b for a, b in zip(provinces, provinces[1:]))
    assert len({minter.mint(seq)[3:] for seq in range(1, 60)}) == 59


def test_province_mix_is_weighted_towards_the_tunnels_own_city():
    minter = PlateMinter(tunnel_index=0, tunnel_count=1, city_code=14, seed=42)
    codes = Counter(minter.mint(seq)[:2] for seq in range(1, 20_000))
    assert codes["14"] / 20_000 > 0.4                      # mostly local traffic
    assert {"34", "06", "35"} <= set(codes)                # and a tail from the big cities
    assert len(codes) > 5


def test_different_tunnels_show_different_local_plates():
    a = PlateMinter(tunnel_index=0, tunnel_count=2, city_code=14, seed=42)
    b = PlateMinter(tunnel_index=1, tunnel_count=2, city_code=53, seed=42)
    top_a = Counter(a.mint(s)[:2] for s in range(1, 5000)).most_common(1)[0][0]
    top_b = Counter(b.mint(s)[:2] for s in range(1, 5000)).most_common(1)[0][0]
    assert (top_a, top_b) == ("14", "53")


# --- vehicles ------------------------------------------------------------

def test_every_spawned_vehicle_carries_a_plate_of_its_tunnel():
    cfg = config()
    tunnel = build_tunnel(cfg, 0)
    factory = VehicleFactory(cfg, random.Random("plates"))
    plates = [factory.spawn(tunnel, float(i)).plate for i in range(500)]
    assert all(PLATE.match(p) for p in plates)
    assert len(set(plates)) == 500
    assert Counter(p[:2] for p in plates)["14"] > 200
