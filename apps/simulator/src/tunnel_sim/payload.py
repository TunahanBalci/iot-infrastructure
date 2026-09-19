"""Detection and device health -> JSON payload."""

from __future__ import annotations

import orjson

from .clock import SimClock
from .config import PayloadConfig
from .model import Detection, Sensor

SCHEMA_VERSION = 2


class PayloadEncoder:
    def __init__(self, cfg: PayloadConfig, boot_id: str, clock: SimClock | None = None):
        self.include_vehicle_id = cfg.include_vehicle_id
        self.include_true_class = cfg.include_true_class
        self.boot_id = boot_id
        self.clock = clock if clock is not None else SimClock(0.0)

    def encode(self, d: Detection) -> bytes:
        s, v = d.sensor, d.vehicle
        body = {
            "schema": SCHEMA_VERSION,
            "message_id": f"{s.sensor_id}:{self.boot_id}:{d.seq}",
            "sensor_id": s.sensor_id,
            "device_id": s.device_id,
            "device_serial": s.device_serial,
            "tunnel_id": s.tunnel_id,
            "position": s.position,
            "seq": d.seq,
            "ts": int(self.clock.epoch(d.t_reported) * 1000.0),
            "direction": v.direction,
            "lane": v.lane,
            "plate": v.plate,
            "speed_kmh": round(d.speed_kmh, 1),
            "length_m": round(d.length_m, 2),
            "occupancy_ms": d.occupancy_ms,
            "classification": d.classification,
            "confidence": round(d.confidence, 3),
        }
        if self.include_vehicle_id:
            body["vehicle_id"] = v.vehicle_id
        if self.include_true_class:
            body["true_class"] = v.vtype
        return orjson.dumps(body)


def health_payload(sensor: Sensor, ts_ms: int, window_s: float, uptime_s: float, tunnel_vehicles: int) -> bytes:
    """What a device reports about itself: its own window counters and condition.

    Ground truth from the device's point of view — a degraded unit knows it is degraded.
    Whether the pipeline can *tell* is a separate question, answered by comparing this with
    what consensus observes.
    """
    if sensor.w_published == 0 and tunnel_vehicles > 0:
        status = "silent"
    else:
        status = "degraded" if sensor.degraded else "ok"
    return orjson.dumps({
        "schema": SCHEMA_VERSION,
        "sensor_id": sensor.sensor_id,
        "device_id": sensor.device_id,
        "device_serial": sensor.device_serial,
        "tunnel_id": sensor.tunnel_id,
        "position": sensor.position,
        "ts": ts_ms,
        "status": status,
        "degraded": sensor.degraded,
        "window_s": round(window_s, 1),
        "uptime_s": round(uptime_s, 1),
        "detections": sensor.w_published,
        "duplicates": sensor.w_duplicates,
        "missed": sensor.w_missed,
    })
