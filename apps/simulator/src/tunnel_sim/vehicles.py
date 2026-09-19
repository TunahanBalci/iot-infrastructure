"""Vehicle types: names, physical dimensions and how they relate to a tunnel's speed limit.

Turkish type names written with the corresponding English letters (İ->I, Ö->O, Ü->U,
Ç->C, Ş->S, Ğ->G). SIZE_ORDER sorts them by length: neighbours in this order are the ones
a sensor confuses with each other, which is the realistic classification failure.

BILINMEYEN ("unknown") is a sensor output only — a vehicle is never spawned as one.
"""

from __future__ import annotations

from dataclasses import dataclass

UNKNOWN = "BILINMEYEN"


@dataclass(frozen=True, slots=True)
class Dist:
    mean: float
    std: float
    min: float
    max: float


@dataclass(frozen=True, slots=True)
class TypeSpec:
    length_m: Dist
    speed_offset_kmh: float     # free speed relative to the tunnel's posted limit
    speed_std_kmh: float
    speed_cap_kmh: float        # absolute ceiling of the type (a tractor cannot do 80)


# Ordered by mean length — SIZE_ORDER depends on it.
PHYSICAL: dict[str, TypeSpec] = {
    "MOTOSIKLET":         TypeSpec(Dist(2.1, 0.2, 1.6, 2.9), +6.0, 9.0, 180.0),
    "TRAKTOR":            TypeSpec(Dist(4.0, 0.6, 3.0, 6.0), -40.0, 6.0, 45.0),
    "OTOMOBIL":           TypeSpec(Dist(4.6, 0.5, 3.2, 6.2), 0.0, 8.0, 200.0),
    "HAFIF_TICARI":       TypeSpec(Dist(5.4, 0.4, 4.6, 6.8), -3.0, 7.0, 140.0),
    "MINIBUS":            TypeSpec(Dist(6.5, 0.5, 5.5, 8.0), -4.0, 7.0, 130.0),
    "OZEL_AMACLI_TASIT":  TypeSpec(Dist(7.5, 1.5, 5.5, 11.0), -10.0, 6.0, 110.0),
    "KAMYON":             TypeSpec(Dist(9.5, 2.0, 6.5, 14.0), -10.0, 6.0, 110.0),
    "OTOBUS":             TypeSpec(Dist(12.0, 1.5, 9.5, 15.0), -8.0, 6.0, 110.0),
    "CEKICI_YARI_ROMORK": TypeSpec(Dist(16.5, 1.5, 13.0, 22.0), -12.0, 5.0, 100.0),
}

SIZE_ORDER = tuple(PHYSICAL)
SPAWNABLE_TYPES = SIZE_ORDER
VEHICLE_TYPES = SPAWNABLE_TYPES + (UNKNOWN,)
SIZE_INDEX = {t: i for i, t in enumerate(SIZE_ORDER)}

# Length decision boundaries: midpoints between neighbouring means. A sensor's confidence
# is how far its length estimate sits from the nearest boundary.
BOUNDARIES = tuple((PHYSICAL[a].length_m.mean + PHYSICAL[b].length_m.mean) / 2
                   for a, b in zip(SIZE_ORDER, SIZE_ORDER[1:]))

MIN_SPEED_KMH = 12.0
CRUISE_MARGIN_KMH = 6.0   # traffic cruises a little under the posted limit


def speed_dist(vehicle_type: str, speed_limit_kmh: float) -> Dist:
    """Free-flow speed distribution of `vehicle_type` in a tunnel posting `speed_limit_kmh`.

    Centred just under the limit (plus the type's offset) and hard-capped at it: normal traffic
    drives close to the limit without breaking it, and the two or three daily speeders are set
    explicitly when the vehicle is spawned, not drawn from this tail.
    """
    spec = PHYSICAL[vehicle_type]
    mean = min(speed_limit_kmh + spec.speed_offset_kmh - CRUISE_MARGIN_KMH, spec.speed_cap_kmh)
    hi = min(speed_limit_kmh, spec.speed_cap_kmh)
    lo = max(MIN_SPEED_KMH, min(mean - 3 * spec.speed_std_kmh, hi - 1.0))
    return Dist(mean, spec.speed_std_kmh, lo, hi)


def neighbour_of(vehicle_type: str, pick_higher: bool) -> str:
    """The adjacent type in SIZE_ORDER — what a sensor reports when it gets it wrong."""
    i = SIZE_INDEX[vehicle_type]
    if i == 0:
        return SIZE_ORDER[1]
    if i == len(SIZE_ORDER) - 1:
        return SIZE_ORDER[i - 1]
    return SIZE_ORDER[i + 1] if pick_higher else SIZE_ORDER[i - 1]


def boundary_margin(length_m: float) -> float:
    """Distance from `length_m` to the nearest decision boundary."""
    return min(abs(length_m - b) for b in BOUNDARIES)
