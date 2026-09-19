"""Device identity: every message says which physical device sent it."""

import random
import re

from tunnel_sim.config import Config
from tunnel_sim.devices import mac_for, serial_for
from tunnel_sim.model import VehicleFactory, build_tunnel, detect
from tunnel_sim.payload import SCHEMA_VERSION, PayloadEncoder
from tunnel_sim.sinks import publish_properties
from tunnel_sim.vehicles import VEHICLE_TYPES

MAC = re.compile(r"^02:[0-9a-f]{2}(:[0-9a-f]{2}){4}$")
SERIAL = re.compile(r"^TSN-[0-9A-Z]{10}$")

PROFILE = {
    "tunnel_id": "TR-TEST", "name": "Test", "city": "Bolu", "city_code": 14,
    "length_m": 2000.0, "lanes_per_direction": 2, "speed_limit_kmh": 80.0,
    "type_mix": {"OTOMOBIL": 1.0},
}


def config(**overrides) -> Config:
    return Config.model_validate({"profiles": [PROFILE], "sensors": {"miss_rate": 0, "duplicate_rate": 0},
                                  **overrides})


# --- addresses -----------------------------------------------------------

def test_mac_is_locally_administered_and_unicast():
    mac = mac_for("TR-TEST-start", seed=42)
    assert MAC.match(mac)
    first = int(mac[:2], 16)
    assert first & 0x02 and not first & 0x01   # locally administered, unicast


def test_mac_and_serial_are_stable_for_a_device_and_differ_between_devices():
    assert mac_for("TR-TEST-start", 42) == mac_for("TR-TEST-start", 42)
    assert serial_for("TR-TEST-start", 42) == serial_for("TR-TEST-start", 42)
    devices = [f"TR-{t}-{p}" for t in range(200) for p in ("start", "middle", "end")]
    assert len({mac_for(d, 42) for d in devices}) == len(devices)
    assert len({serial_for(d, 42) for d in devices}) == len(devices)
    assert SERIAL.match(serial_for("TR-TEST-start", 42))


def test_a_different_seed_gives_a_different_device():
    assert mac_for("TR-TEST-start", 42) != mac_for("TR-TEST-start", 7)


def test_sensors_carry_their_device_identity():
    tunnel = build_tunnel(config(), 0)
    for s in tunnel.sensors:
        assert s.device_id == mac_for(s.sensor_id, 42)
        assert s.device_serial == serial_for(s.sensor_id, 42)
    assert len({s.device_id for s in tunnel.sensors}) == 3


# --- payload -------------------------------------------------------------

def detection_payload(cfg: Config) -> dict:
    import orjson
    tunnel = build_tunnel(cfg, 0)
    v = VehicleFactory(cfg, random.Random("dev")).spawn(tunnel, 0.0)
    d = detect(v.order[0], v, random.Random("det"))
    return orjson.loads(PayloadEncoder(cfg.payload, "b00t").encode(d))


def test_payload_is_schema_2_and_names_the_device_and_the_plate():
    body = detection_payload(config())
    assert SCHEMA_VERSION == 2 and body["schema"] == 2
    assert MAC.match(body["device_id"]) and SERIAL.match(body["device_serial"])
    assert body["sensor_id"] == f"TR-TEST-{body['position']}" and body["tunnel_id"] == "TR-TEST"
    assert re.match(r"^\d{2} [A-Z]{1,3} \d{2,4}$", body["plate"])
    assert body["classification"] in VEHICLE_TYPES
    for field in ("message_id", "position", "seq", "ts", "direction", "lane",
                  "speed_kmh", "length_m", "occupancy_ms", "confidence"):
        assert field in body, field


def test_ground_truth_fields_stay_opt_in():
    assert "true_class" not in detection_payload(config())
    body = detection_payload(config(payload={"include_vehicle_id": True, "include_true_class": True}))
    assert body["true_class"] in VEHICLE_TYPES and body["vehicle_id"].startswith("TR-TEST-V")


def test_mqtt_user_properties_carry_the_device_id():
    tunnel = build_tunnel(config(), 0)
    cfg = config()
    props = publish_properties(cfg.mqtt, tunnel.sensors[0])
    rendered = str(props)
    assert "device_id" in rendered and tunnel.sensors[0].device_id in rendered
    assert "schema" in rendered and "'2'" in rendered or '"2"' in rendered
