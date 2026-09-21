"""Detection message (simulator schema 1) and per-sensor bookkeeping."""

from __future__ import annotations

import sys
from collections import deque

import orjson

SCHEMA_VERSION = 2
START_TO_END = "start_to_end"
END_TO_START = "end_to_start"
UNKNOWN = "BILINMEYEN"
# The types a device can report (apps/simulator/src/tunnel_sim/vehicles.py). UNKNOWN means the
# device could not tell, which is an abstention in the vote, not a class of its own.
VEHICLE_TYPES = (
    "MOTOSIKLET", "TRAKTOR", "OTOMOBIL", "HAFIF_TICARI", "MINIBUS", "OZEL_AMACLI_TASIT",
    "KAMYON", "OTOBUS", "CEKICI_YARI_ROMORK", UNKNOWN,
)
CLASSES = frozenset(VEHICLE_TYPES)

# Index of each sensor in the order a vehicle passes it.
TRAVEL_INDEX = {
    START_TO_END: {"start": 0, "middle": 1, "end": 2},
    END_TO_START: {"end": 0, "middle": 1, "start": 2},
}
POSITION_AT = {d: tuple(sorted(m, key=m.__getitem__)) for d, m in TRAVEL_INDEX.items()}
# Pending detections are held for minutes: share repeated strings instead of one copy per detection.
_CANONICAL = {s: s for s in (START_TO_END, END_TO_START, "start", "middle", "end", *VEHICLE_TYPES)}
_intern = sys.intern


class InvalidDetection(ValueError):
    pass


class Detection:
    __slots__ = ("message_id", "sensor_id", "device_id", "tunnel_id", "position", "boot_id", "seq", "ts",
                 "direction", "index", "lane", "plate", "speed_ms", "length_m", "vtype", "confidence",
                 "vehicle_id", "true_class", "used", "offset", "trace")

    def __init__(self, message_id: str, sensor_id: str, tunnel_id: str, position: str, boot_id: str, seq: int,
                 ts: float, direction: str, lane: int, speed_ms: float, length_m: float, vtype: str,
                 confidence: float, plate: str = "", device_id: str = "",
                 vehicle_id: str | None = None, true_class: str | None = None):
        self.message_id = message_id
        self.sensor_id = sensor_id
        self.device_id = device_id          # physical address of the device that reported it
        self.tunnel_id = tunnel_id
        self.position = position
        self.boot_id = boot_id
        self.seq = seq
        self.ts = ts                      # epoch seconds
        self.direction = direction
        self.index = TRAVEL_INDEX[direction][position]
        self.lane = lane
        self.plate = plate
        self.speed_ms = speed_ms
        self.length_m = length_m
        self.vtype = vtype                  # type this sensor reported (UNKNOWN = could not tell)
        self.confidence = confidence
        self.vehicle_id = vehicle_id      # ground truth, only in simulator evaluation mode
        self.true_class = true_class
        self.used = False                 # consumed by an emitted vehicle event
        self.offset = -1                  # Kafka offset of the record (Kafka input only)
        self.trace: bytes | None = None   # sampled W3C traceparent of the record (tracing only)


def parse_detection(payload: bytes) -> Detection:
    try:
        d = orjson.loads(payload)
        if d["schema"] != SCHEMA_VERSION:
            raise InvalidDetection(f"unsupported schema {d['schema']!r}")
        message_id = d["message_id"]
        direction = d["direction"]
        position = d["position"]
        classification = d["classification"]
        if direction not in TRAVEL_INDEX or position not in TRAVEL_INDEX[direction]:
            raise InvalidDetection(f"bad direction/position {direction!r}/{position!r}")
        if classification not in CLASSES:
            raise InvalidDetection(f"bad classification {classification!r}")
        speed_kmh = float(d["speed_kmh"])
        if not speed_kmh > 0:
            raise InvalidDetection("speed_kmh must be > 0")
        # message_id = sensor_id:boot_id:seq
        parts = message_id.rsplit(":", 2)
        boot_id = parts[1] if len(parts) == 3 else ""
        return Detection(
            message_id=message_id,
            sensor_id=_intern(d["sensor_id"]),
            tunnel_id=_intern(d["tunnel_id"]),
            position=_CANONICAL[position],
            boot_id=_intern(boot_id),
            seq=int(d["seq"]),
            ts=d["ts"] / 1000.0,
            direction=_CANONICAL[direction],
            lane=int(d["lane"]),
            plate=d["plate"],
            speed_ms=speed_kmh / 3.6,
            length_m=float(d["length_m"]),
            vtype=_CANONICAL[classification],
            confidence=float(d["confidence"]),
            device_id=_intern(d.get("device_id", "")),
            vehicle_id=d.get("vehicle_id"),
            true_class=d.get("true_class"),
        )
    except InvalidDetection:
        raise
    except (orjson.JSONDecodeError, KeyError, TypeError, ValueError, AttributeError) as e:
        raise InvalidDetection(f"{type(e).__name__}: {e}") from None


class SensorState:
    """Duplicate suppression, message-loss accounting and learned measurement noise of one sensor.

    The devices report their own condition (simulator -> tunnels/+/sensors/+/health), so nothing
    here judges a sensor's reliability any more; the noise estimates stay because the association
    gates are built from them.
    """

    __slots__ = ("sensor_id", "position", "boot_id", "max_seq", "recent", "recent_order", "window",
                 "speed_var", "length_var", "lost_total", "w_detections", "w_duplicates", "w_lost")

    def __init__(self, sensor_id: str, position: str, window: int, speed_noise: float,
                 length_noise: float = 0.6):
        self.sensor_id = sensor_id
        self.position = position
        self.boot_id: str | None = None
        self.max_seq = 0
        self.recent: set[int] = set()
        self.recent_order: deque[int] = deque()
        self.window = window
        self.speed_var = speed_noise * speed_noise     # learned relative speed error variance
        self.length_var = length_noise * length_noise  # learned length error variance (m^2)
        self.lost_total = 0
        self.reset_window()

    def reset_window(self) -> None:
        self.w_detections = 0
        self.w_duplicates = 0
        self.w_lost = 0       # seq gaps: published but never received (QoS 0 loss)

    def accept(self, boot_id: str, seq: int) -> bool:
        """False if (boot_id, seq) was already seen (duplicate)."""
        if boot_id != self.boot_id:  # first message or sensor restarted
            self.boot_id = boot_id
            self.max_seq = seq
            self.recent.clear()
            self.recent_order.clear()
        elif seq in self.recent or seq <= self.max_seq - self.window:
            self.w_duplicates += 1
            return False
        elif seq > self.max_seq:
            gap = seq - self.max_seq - 1
            self.w_lost += gap
            self.lost_total += gap
            self.max_seq = seq
        elif self.lost_total > 0:  # late arrival of a seq already counted as lost
            self.lost_total -= 1
            self.w_lost = max(0, self.w_lost - 1)
        self.recent.add(seq)
        self.recent_order.append(seq)
        if len(self.recent_order) > self.window:
            self.recent.discard(self.recent_order.popleft())
        self.w_detections += 1
        return True
