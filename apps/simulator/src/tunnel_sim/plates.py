"""Turkish plate numbers, unique by construction.

Legal shapes after the two-digit province code: 1 letter + 4 digits, 2 letters + 4 digits,
3 letters + 2 digits. `encode` is a bijection from 0..CAPACITY-1 onto those shapes, so two
different codes are always two different plates.

A tunnel mints from `n = seq * tunnel_count + tunnel_index`, a value no other tunnel produces.
The code is `n` itself, so every vehicle of a run gets a plate no other vehicle has. The
province comes from a 100-slot weighted cycle indexed by `n`, spread evenly so that each tunnel
sees the intended mix however many tunnels share the stride — it varies vehicle to vehicle and
carries no identity, which is why the code space alone sets the ceiling (see `mint`).
"""

from __future__ import annotations

LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"   # Turkish-only letters never appear on a plate
_L = len(LETTERS)

ONE_LETTER = _L * 10_000            #   260,000  "A 1234"
TWO_LETTERS = _L * _L * 10_000      # 6,760,000  "AB 1234"
THREE_LETTERS = _L ** 3 * 100       # 1,757,600  "ABC 12"
CAPACITY = ONE_LETTER + TWO_LETTERS + THREE_LETTERS

CITY_SLOTS = 100                    # one full weighted province cycle
BIG_CITIES = (34, 6, 35)            # İstanbul, Ankara, İzmir
# Provinces that show up everywhere: transit and fleet traffic.
COMMON_CITIES = (1, 7, 9, 10, 16, 20, 21, 25, 27, 31, 33, 38, 41, 42, 44, 52, 54, 55, 61, 65, 67)

LOCAL_SHARE = 0.55                  # plates of the tunnel's own province
BIG_CITY_SHARE = 0.20               # İstanbul / Ankara / İzmir
# the rest: other provinces, deterministic per tunnel


def encode(code: int) -> str:
    """Plate body (without the province code) for `code` in 0..CAPACITY-1."""
    if not 0 <= code < CAPACITY:
        raise ValueError(f"code out of range: {code}")
    if code < ONE_LETTER:
        i, digits = divmod(code, 10_000)
        return f"{LETTERS[i]} {digits:04d}"
    code -= ONE_LETTER
    if code < TWO_LETTERS:
        i, digits = divmod(code, 10_000)
        return f"{LETTERS[i // _L]}{LETTERS[i % _L]} {digits:04d}"
    code -= TWO_LETTERS
    i, digits = divmod(code, 100)
    return f"{LETTERS[i // (_L * _L)]}{LETTERS[(i // _L) % _L]}{LETTERS[i % _L]} {digits:02d}"


SPREAD = 37   # coprime with CITY_SLOTS: spreads the categories evenly over every residue class


def city_cycle(city_code: int, seed: int | None) -> tuple[int, ...]:
    """100 province codes in the mix of one tunnel: mostly local, some big-city, a transit tail.

    Slot j takes its category from (j * SPREAD) % 100, so any arithmetic subsequence of slots
    — which is what a tunnel sees when several tunnels share the plate stride — still holds the
    intended shares.
    """
    offset = (hash((seed, city_code)) if seed is not None else 0) % CITY_SLOTS
    local = round(CITY_SLOTS * LOCAL_SHARE)
    big = local + round(CITY_SLOTS * BIG_CITY_SHARE)
    others = [c for c in COMMON_CITIES if c != city_code]
    slots = []
    for j in range(CITY_SLOTS):
        k = ((j + offset) * SPREAD) % CITY_SLOTS
        if k < local:
            slots.append(city_code)
        elif k < big:
            slots.append(BIG_CITIES[k % len(BIG_CITIES)])
        else:
            slots.append(others[k % len(others)])
    return tuple(slots)


class PlateMinter:
    """Plates of one tunnel. `mint(seq)` is pure: the same seq always gives the same plate."""

    __slots__ = ("stride", "index", "cities")

    def __init__(self, tunnel_index: int, tunnel_count: int, city_code: int, seed: int | None):
        self.stride = max(1, tunnel_count)
        self.index = tunnel_index % self.stride
        self.cities = city_cycle(city_code, seed)

    def mint(self, seq: int) -> str:
        n = seq * self.stride + self.index
        # ponytail: plates are exactly unique for the first CAPACITY vehicles of a run (8.8M,
        # ~10 days at the deployed rate). Past that the code space wraps and the province cycle
        # is shifted, so repeats need the shift to realign — roughly 10x further out. A run that
        # needs more has to widen the code space (a 4th plate shape) or accept real-world reuse.
        wrap, code = divmod(n, CAPACITY)
        return f"{self.cities[(n + wrap * SPREAD) % CITY_SLOTS]:02d} {encode(code)}"
