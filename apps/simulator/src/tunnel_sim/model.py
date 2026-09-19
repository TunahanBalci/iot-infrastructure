"""Tunnel / vehicle / sensor domain model and sensor physics.

Coordinate system: a tunnel spans x in [0, length]. Sensors sit at
start (x=0), middle (x=length/2) and end (x=length). A vehicle travelling
START_TO_END has its front at x=0 at `t_entry`; END_TO_START at x=length.
The front reaches a sensor at t_entry + distance / speed, so the detection
order and spacing always follow the vehicle's speed and direction.

A tunnel is built from a profile: the curated ones in config/tunnels.yaml, or a
synthetic profile derived from the seed in generator mode.
"""

from __future__ import annotations

import random
from bisect import bisect
from dataclasses import dataclass

from .clock import SimClock
from .config import POSITIONS, Config, Distribution, SensorErrorModel
from .devices import mac_for, serial_for
from .plates import PlateMinter
from .profiles import TunnelProfile
from .vehicles import (
    PHYSICAL,
    SIZE_ORDER,
    UNKNOWN,
    Dist,
    boundary_margin,
    neighbour_of,
    speed_dist,
)

START_TO_END = "start_to_end"
END_TO_START = "end_to_start"

POSITION_FRACTION = {"start": 0.0, "middle": 0.5, "end": 1.0}

# Generator mode only: synthetic profiles for load testing.
GENERATOR_CITIES = (
    ("Adana", 1), ("Ankara", 6), ("Antalya", 7), ("Artvin", 8), ("Aydın", 9),
    ("Balıkesir", 10), ("Bolu", 14), ("Bursa", 16), ("Denizli", 20), ("Erzincan", 24),
    ("Erzurum", 25), ("Gümüşhane", 29), ("İstanbul", 34), ("İzmir", 35), ("Kastamonu", 37),
    ("Konya", 42), ("Rize", 53), ("Sivas", 58), ("Trabzon", 61), ("Zonguldak", 67),
)
GENERATOR_SPEED_LIMITS = (70.0, 80.0, 90.0, 100.0)
GENERATOR_TYPE_MIX = {
    "OTOMOBIL": 55.0, "HAFIF_TICARI": 12.0, "KAMYON": 11.0, "CEKICI_YARI_ROMORK": 9.0,
    "OTOBUS": 5.0, "MINIBUS": 5.0, "MOTOSIKLET": 2.0, "OZEL_AMACLI_TASIT": 0.7, "TRAKTOR": 0.3,
}


def sensor_id_for(tunnel_id: str, position: str) -> str:
    return f"{tunnel_id}-{position}"


def truncated_gauss(rng: random.Random, d: Distribution | Dist) -> float:
    if d.std == 0:
        return min(max(d.mean, d.min), d.max)
    for _ in range(16):
        v = rng.gauss(d.mean, d.std)
        if d.min <= v <= d.max:
            return v
    return min(max(rng.gauss(d.mean, d.std), d.min), d.max)


class Sensor:
    __slots__ = ("sensor_id", "device_id", "device_serial", "tunnel_id", "position", "x_m", "degraded",
                 "err", "seq", "topic", "health_topic", "sink_ref",
                 "published", "missed", "duplicates", "dropped",       # totals, for metrics
                 "w_published", "w_missed", "w_duplicates")            # current health window

    def __init__(self, tunnel_id: str, position: str, x_m: float, degraded: bool, err: SensorErrorModel,
                 topic: str, seed: int | None = None, health_topic: str = ""):
        self.sensor_id = sensor_id_for(tunnel_id, position)
        self.device_id = mac_for(self.sensor_id, seed)          # physical address of the device
        self.device_serial = serial_for(self.sensor_id, seed)
        self.tunnel_id = tunnel_id
        self.position = position
        self.x_m = x_m
        self.degraded = degraded
        self.err = err
        self.seq = 0
        self.topic = topic
        self.health_topic = health_topic
        self.sink_ref: object = None  # opaque per-sink handle (e.g. MQTT connection + properties)
        self.published = self.missed = self.duplicates = self.dropped = 0
        self.w_published = self.w_missed = self.w_duplicates = 0

    def reset_window(self) -> None:
        self.w_published = self.w_missed = self.w_duplicates = 0


class Tunnel:
    __slots__ = ("index", "tunnel_id", "profile", "length_m", "lanes_per_direction", "rate_multiplier",
                 "sensors", "vehicle_seq", "plates", "speeding_prob", "_types", "_cum_weights",
                 "_speed", "_length", "_rule_minute", "_rule_cum")

    def __init__(self, index: int, profile: TunnelProfile, rate_multiplier: float = 1.0,
                 plates: PlateMinter | None = None):
        self.index = index
        self.profile = profile
        self.tunnel_id = profile.tunnel_id
        self.length_m = profile.length_m
        self.lanes_per_direction = profile.lanes_per_direction
        self.rate_multiplier = rate_multiplier
        self.sensors: list[Sensor] = []
        self.vehicle_seq = 0
        self.plates = plates or PlateMinter(index, 1, profile.city_code, None)
        self.speeding_prob = 0.0        # set by build_tunnel from traffic.speeding_per_day
        self._rule_minute = -1
        self._rule_cum: tuple[float, ...] = ()
        # Spawn mix and per-type distributions, resolved once: the hot path only bisects.
        self._types = tuple(t for t, w in profile.type_mix.items() if w > 0)
        total = 0.0
        cum = []
        for t in self._types:
            total += profile.type_mix[t]
            cum.append(total)
        self._cum_weights = tuple(cum)
        self._speed = {t: speed_dist(t, profile.speed_limit_kmh) for t in self._types}
        self._length = {t: PHYSICAL[t].length_m for t in self._types}

    @property
    def speed_limit_kmh(self) -> float:
        return self.profile.speed_limit_kmh

    def sensors_in_travel_order(self, direction: str) -> list[Sensor]:
        return self.sensors if direction == START_TO_END else self.sensors[::-1]

    def pick_type(self, rng: random.Random, cum: tuple[float, ...] | None = None) -> str:
        """Draw a vehicle type from cumulative weights (the tunnel's mix by default)."""
        cum = self._cum_weights if cum is None else cum
        return self._types[min(bisect(cum, rng.random() * cum[-1]), len(self._types) - 1)]

    def cum_weights_at(self, minute_of_day: int, violation_factor: float) -> tuple[float, ...]:
        """Cumulative spawn weights for that local minute: banned types are scaled down, and the
        share they lose goes to the types that are allowed — the tunnel keeps its message rate."""
        if minute_of_day == self._rule_minute:
            return self._rule_cum
        forbidden = self.profile.forbidden_types(minute_of_day // 60, minute_of_day % 60)
        if not forbidden:
            cum = self._cum_weights
        else:
            mix, total, out = self.profile.type_mix, 0.0, []
            for t in self._types:
                total += mix[t] * violation_factor if t in forbidden else mix[t]
                out.append(total)
            cum = tuple(out) if total > 0 else self._cum_weights   # everything banned: keep traffic
        self._rule_minute, self._rule_cum = minute_of_day, cum
        return cum

    @property
    def types(self) -> tuple[str, ...]:
        return self._types


class Vehicle:
    __slots__ = ("vehicle_id", "tunnel", "vtype", "plate", "length_m", "speed_ms", "direction", "lane",
                 "t_entry", "order", "speeding")

    def __init__(self, vehicle_id: str, tunnel: Tunnel, vtype: str, length_m: float, speed_ms: float,
                 direction: str, lane: int, t_entry: float, plate: str = "", speeding: bool = False):
        self.vehicle_id = vehicle_id
        self.tunnel = tunnel
        self.vtype = vtype
        self.plate = plate
        self.length_m = length_m
        self.speed_ms = speed_ms
        self.direction = direction
        self.lane = lane
        self.t_entry = t_entry
        self.speeding = speeding
        self.order = tunnel.sensors_in_travel_order(direction)

    def distance_to(self, sensor: Sensor) -> float:
        if self.direction == START_TO_END:
            return sensor.x_m
        return self.tunnel.length_m - sensor.x_m

    def time_at(self, sensor: Sensor) -> float:
        """True time the vehicle's front reaches `sensor`."""
        return self.t_entry + self.distance_to(sensor) / self.speed_ms

    @property
    def t_exit(self) -> float:
        return self.t_entry + (self.tunnel.length_m + self.length_m) / self.speed_ms


def generated_profile(cfg: Config, index: int, rng: random.Random) -> TunnelProfile:
    """Synthetic profile for generator mode: no rules, so the hot path stays cheap."""
    topo = cfg.topology
    city, code = GENERATOR_CITIES[index % len(GENERATOR_CITIES)]
    return TunnelProfile(
        tunnel_id=topo.tunnel_id_format.format(index=index),
        name=f"{city} Tüneli {index}",
        city=city,
        city_code=code,
        length_m=rng.uniform(*topo.length_m),
        lanes_per_direction=rng.randint(*topo.lanes_per_direction),
        speed_limit_kmh=rng.choice(GENERATOR_SPEED_LIMITS),
        type_mix=dict(GENERATOR_TYPE_MIX),
    )


def build_tunnel(cfg: Config, index: int) -> Tunnel:
    """Deterministically build tunnel `index` (independent of worker sharding)."""
    seed = cfg.simulation.seed
    rng = random.Random(f"tunnel:{seed}:{index}") if seed is not None else random.Random()
    traffic, scfg = cfg.traffic, cfg.sensors
    if cfg.profiles:
        profile = cfg.profiles[index % len(cfg.profiles)]
    else:
        profile = generated_profile(cfg, index, rng)
    plates = PlateMinter(index, cfg.tunnel_count(), profile.city_code, seed)
    tunnel = Tunnel(index, profile, rng.uniform(*traffic.rate_variation), plates)
    # A couple of speeders a day, drawn per tunnel: a per-vehicle probability, so the daily
    # count varies around the target instead of being exactly the same every day.
    tunnel.speeding_prob = rng.uniform(*traffic.speeding_per_day) / traffic.expected_daily_vehicles(
        tunnel.rate_multiplier)
    for position in POSITIONS:
        degraded = rng.random() < scfg.degraded_ratio
        sid = sensor_id_for(profile.tunnel_id, position)
        fields = {"tunnel_id": profile.tunnel_id, "position": position, "sensor_id": sid}
        tunnel.sensors.append(Sensor(profile.tunnel_id, position, profile.length_m * POSITION_FRACTION[position],
                                     degraded, scfg.error_model(position, degraded),
                                     cfg.mqtt.topic_template.format(**fields), seed,
                                     cfg.mqtt.health_topic_template.format(**fields)))
    return tunnel


class VehicleFactory:
    def __init__(self, cfg: Config, rng: random.Random, clock: SimClock | None = None):
        self.traffic = cfg.traffic
        self.rng = rng
        self.clock = clock if clock is not None else SimClock(0.0)
        # Generator mode is a load generator: a steady rate and no rules, so its hot path never
        # touches the clock. The daily curve and the access rules belong to the profile tunnels.
        self.daily_curve = cfg.topology.mode == "profiles"

    def rate(self, tunnel: Tunnel, sim_t: float) -> float:
        """Vehicles per second in `tunnel` at simulation time `sim_t`.

        One vehicle is one detection per device, so this is also the per-device message rate
        (before the sensors' own misses and duplicates)."""
        if not self.daily_curve:
            return self.traffic.msgs_per_device_s * tunnel.rate_multiplier
        return self.traffic.rate_at(self.clock.minute_of_day(sim_t)) * tunnel.rate_multiplier

    def next_arrival_gap(self, tunnel: Tunnel, sim_t: float = 0.0) -> float:
        return self.rng.expovariate(self.rate(tunnel, sim_t))

    def spawn(self, tunnel: Tunnel, t_entry: float) -> Vehicle:
        rng, tr = self.rng, self.traffic
        if tunnel.profile.rules:
            cum = tunnel.cum_weights_at(self.clock.minute_of_day(t_entry), tr.violation_factor)
        else:
            cum = None
        vtype = tunnel.pick_type(rng, cum)
        limit = tunnel.profile.speed_limit_kmh
        speeding = rng.random() < tunnel.speeding_prob and PHYSICAL[vtype].speed_cap_kmh > limit
        speed_kmh = min(limit * rng.uniform(1.15, 1.45), PHYSICAL[vtype].speed_cap_kmh) if speeding \
            else truncated_gauss(rng, tunnel._speed[vtype])
        tunnel.vehicle_seq += 1
        return Vehicle(
            vehicle_id=f"{tunnel.tunnel_id}-V{tunnel.vehicle_seq}",
            tunnel=tunnel,
            vtype=vtype,
            plate=tunnel.plates.mint(tunnel.vehicle_seq),
            length_m=truncated_gauss(rng, tunnel._length[vtype]),
            speed_ms=speed_kmh / 3.6,
            speeding=speeding,
            direction=START_TO_END if rng.random() < tr.start_to_end_ratio else END_TO_START,
            lane=rng.randint(1, tunnel.lanes_per_direction),
            t_entry=t_entry,
        )


@dataclass(slots=True)
class Detection:
    sensor: Sensor
    vehicle: Vehicle
    seq: int
    t_true: float           # physical time front passed sensor (epoch s)
    t_reported: float       # with timestamp jitter
    speed_kmh: float        # measured
    length_m: float         # estimated
    occupancy_ms: int       # time vehicle covered sensor, consistent with measured speed & length
    classification: str     # one of vehicles.VEHICLE_TYPES
    confidence: float


def classify(vtype: str, est_len: float, rng: random.Random, err: SensorErrorModel) -> tuple[str, float]:
    """What the sensor reports for a vehicle of true type `vtype`, and how sure it is.

    Ambiguous readings become BILINMEYEN; mistakes land on a neighbouring size class,
    which is how a length/profile classifier actually fails.
    """
    if err.unknown_rate and rng.random() < err.unknown_rate:
        return UNKNOWN, rng.uniform(0.2, 0.5)
    if err.misclassification_rate and rng.random() < err.misclassification_rate:
        return neighbour_of(vtype, rng.random() < 0.5), rng.uniform(0.45, 0.79)
    # Confidence grows with distance from the nearest decision boundary, relative to the
    # sensor's own length noise: ~0.5 at the boundary, ~0.99 for an unambiguous reading.
    margin = boundary_margin(est_len)
    confidence = 0.5 + 0.49 * (1.0 - 2.718281828 ** (-margin / max(err.length_noise_std_m, 0.05)))
    return vtype, confidence


def detect(sensor: Sensor, vehicle: Vehicle, rng: random.Random) -> Detection | None:
    """Simulate `sensor` observing `vehicle`. Returns None on a missed detection."""
    err = sensor.err
    if err.miss_rate and rng.random() < err.miss_rate:
        return None
    t_true = vehicle.time_at(sensor)
    t_rep = t_true + (rng.gauss(0.0, err.timestamp_jitter_ms / 1000.0) if err.timestamp_jitter_ms else 0.0)
    speed = vehicle.speed_ms * (1.0 + (rng.gauss(0.0, err.speed_noise_ratio) if err.speed_noise_ratio else 0.0))
    speed = max(speed, 0.1)
    est_len = vehicle.length_m + (rng.gauss(0.0, err.length_noise_std_m) if err.length_noise_std_m else 0.0)
    est_len = max(est_len, 0.5)

    classification, confidence = classify(vehicle.vtype, est_len, rng, err)

    sensor.seq += 1
    return Detection(
        sensor=sensor,
        vehicle=vehicle,
        seq=sensor.seq,
        t_true=t_true,
        t_reported=t_rep,
        speed_kmh=speed * 3.6,
        length_m=est_len,
        occupancy_ms=int(round(est_len / speed * 1000.0)),
        classification=classification,
        confidence=confidence,
    )


__all__ = ["END_TO_START", "START_TO_END", "SIZE_ORDER", "Detection", "Sensor", "Tunnel", "Vehicle",
           "VehicleFactory", "build_tunnel", "classify", "detect", "sensor_id_for", "truncated_gauss"]
