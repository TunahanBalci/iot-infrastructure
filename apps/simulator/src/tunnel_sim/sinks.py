"""Output sinks: MQTT 5.0 (paho), stdout, discard."""

from __future__ import annotations

import logging
import ssl
import sys
import threading
import time

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .config import Config, MqttConfig, TlsConfig
from .model import Sensor
from .payload import SCHEMA_VERSION

log = logging.getLogger(__name__)


class PrepackedProperties:
    """MQTT5 PUBLISH properties packed once and reused.

    paho re-encodes Properties on every publish, which costs more than the
    JSON encoding itself. Sensor properties are static, so pack them once.
    paho only calls ``pack()`` on the object passed to ``publish``.
    """

    __slots__ = ("_packed", "_repr")

    def __init__(self, props: Properties):
        self._packed = props.pack()
        self._repr = str(props)

    def pack(self) -> bytes:
        return self._packed

    def __str__(self) -> str:
        return self._repr


class Sink:
    def attach(self, sensor: Sensor) -> None:
        """Prepare per-sensor state (stored on sensor.sink_ref)."""

    def start(self) -> None:
        """Called once after every sensor is attached."""

    def publish(self, sensor: Sensor, payload: bytes) -> bool:
        """Publish; returns False if the message was dropped."""
        raise NotImplementedError

    def publish_retained(self, sensor: Sensor, topic: str, payload: bytes) -> bool:
        """Publish a retained message (device health) on `topic`."""
        return True

    def close(self) -> None:
        pass

    def pending(self) -> int:
        return 0

    def connected(self) -> bool:
        return True

    def clients(self) -> tuple[int, int]:
        """(connected, total) broker connections."""
        return 0, 0

    def dropped(self) -> int:
        """Messages accepted by publish() but dropped later inside the sink."""
        return 0


class DiscardSink(Sink):
    def publish(self, sensor: Sensor, payload: bytes) -> bool:
        return True


class StdoutSink(Sink):
    def __init__(self) -> None:
        self._out = sys.stdout.buffer
        self._lock = threading.Lock()

    def publish(self, sensor: Sensor, payload: bytes) -> bool:
        return self.publish_retained(sensor, sensor.topic, payload)

    def publish_retained(self, sensor: Sensor, topic: str, payload: bytes) -> bool:
        with self._lock:
            self._out.write(topic.encode() + b" " + payload + b"\n")
        return True

    def close(self) -> None:
        self._out.flush()


def create_client(m: MqttConfig, client_id: str, status_topic: str) -> mqtt.Client:
    """MQTT5 client with credentials and the retained "offline" Will; TLS and connecting are up to the caller."""
    c = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
        transport=m.transport,
    )
    if m.username is not None:
        c.username_pw_set(m.username, m.password)
    will_props = Properties(PacketTypes.WILLMESSAGE)
    will_props.ContentType = "text/plain"
    c.will_set(status_topic, b"offline", qos=1, retain=True, properties=will_props)
    return c


def configure_tls(c: mqtt.Client, tls: TlsConfig, certfile: str | None, keyfile: str | None) -> None:
    # load_cert_chain sends everything in certfile, so a leaf + intermediate CA bundle presents the full chain.
    c.tls_set(ca_certs=tls.ca_certs, certfile=certfile, keyfile=keyfile,
              cert_reqs=ssl.CERT_REQUIRED if not tls.insecure else ssl.CERT_NONE)
    c.tls_insecure_set(tls.insecure)


def connect_properties(m: MqttConfig) -> Properties:
    props = Properties(PacketTypes.CONNECT)
    props.SessionExpiryInterval = m.session_expiry_s
    return props


def publish_properties(m: MqttConfig, sensor: Sensor) -> PrepackedProperties:
    props = Properties(PacketTypes.PUBLISH)
    props.PayloadFormatIndicator = 1
    props.ContentType = "application/json"
    if m.message_expiry_s:
        props.MessageExpiryInterval = m.message_expiry_s
    if m.user_properties:
        props.UserProperty = [
            ("tunnel_id", sensor.tunnel_id),
            ("sensor_id", sensor.sensor_id),
            ("device_id", sensor.device_id),
            ("position", sensor.position),
            ("schema", str(SCHEMA_VERSION)),
        ]
    return PrepackedProperties(props)


def out_queue_len(c: mqtt.Client) -> int:
    # paho has no public accessor for the outgoing packet buffer; len() of a deque is atomic.
    out = getattr(c, "_out_packet", None)
    return len(out) if out is not None else 0


class _Connection:
    def __init__(self, cfg: Config, client_id: str):
        m = cfg.mqtt
        self.client_id = client_id
        self.status_topic = m.status_topic_template.format(client_id=client_id)
        self.connected_event = threading.Event()
        self.closing = False
        self.client = c = create_client(m, client_id, self.status_topic)
        if m.tls.enabled:
            configure_tls(c, m.tls, m.tls.certfile, m.tls.keyfile)
        c.reconnect_delay_set(*m.reconnect_delay_s)
        c.max_queued_messages_set(m.max_pending_messages)

        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect

        c.connect_async(m.host, m.port, keepalive=m.keepalive_s, clean_start=True, properties=connect_properties(m))
        c.loop_start()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("%s: connect refused: %s", self.client_id, reason_code)
            return
        max_qos = getattr(properties, "MaximumQoS", None) if properties else None
        log.info("%s: connected (server max QoS=%s)", self.client_id, 2 if max_qos is None else max_qos)
        client.publish(self.status_topic, b"online", qos=1, retain=True)
        self.connected_event.set()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected_event.clear()
        if self.closing:
            log.debug("%s: disconnected", self.client_id)
        else:
            log.warning("%s: disconnected: %s (reconnecting)", self.client_id, reason_code)

    def pending(self) -> int:
        return out_queue_len(self.client)

    def close(self) -> None:
        c = self.client
        self.closing = True
        if self.connected_event.is_set():
            c.publish(self.status_topic, b"offline", qos=1, retain=True).wait_for_publish(timeout=5)
        c.disconnect()
        c.loop_stop()


class MqttSink(Sink):
    """connection_mode: shared. connections_per_worker connections, sensors assigned round-robin."""

    def __init__(self, cfg: Config, worker_id: int):
        m = cfg.mqtt
        self.mqtt = m
        self.qos = m.qos
        self.retain = m.retain
        self.max_pending = m.max_pending_messages
        self.conns = [
            _Connection(cfg, f"{m.client_id_prefix}-w{worker_id}-c{i}")
            for i in range(m.connections_per_worker)
        ]
        self._next_conn = 0
        deadline = time.monotonic() + m.connect_timeout_s
        for conn in self.conns:
            if not conn.connected_event.wait(max(0.0, deadline - time.monotonic())):
                self.close()
                raise ConnectionError(f"could not connect to MQTT broker {m.host}:{m.port} "
                                      f"within {m.connect_timeout_s}s")

    def attach(self, sensor: Sensor) -> None:
        conn = self.conns[self._next_conn % len(self.conns)]
        self._next_conn += 1
        sensor.sink_ref = (conn, publish_properties(self.mqtt, sensor))

    def publish(self, sensor: Sensor, payload: bytes) -> bool:
        conn, props = sensor.sink_ref  # type: ignore[misc]
        if conn.pending() >= self.max_pending:
            return False
        info = conn.client.publish(sensor.topic, payload, self.qos, self.retain, properties=props)
        return info.rc == mqtt.MQTT_ERR_SUCCESS

    def publish_retained(self, sensor: Sensor, topic: str, payload: bytes) -> bool:
        conn, props = sensor.sink_ref  # type: ignore[misc]
        info = conn.client.publish(topic, payload, qos=1, retain=True, properties=props)
        return info.rc == mqtt.MQTT_ERR_SUCCESS

    def pending(self) -> int:
        return sum(c.pending() for c in self.conns)

    def connected(self) -> bool:
        return all(c.connected_event.is_set() for c in self.conns)

    def clients(self) -> tuple[int, int]:
        return sum(c.connected_event.is_set() for c in self.conns), len(self.conns)

    def close(self) -> None:
        for c in self.conns:
            try:
                c.close()
            except Exception:  # best effort on shutdown
                log.exception("%s: error during close", c.client_id)


def make_sink(cfg: Config, worker_id: int, n_workers: int = 1) -> Sink:
    if cfg.output.sink == "mqtt":
        if cfg.mqtt.connection_mode == "per_tunnel":
            from .per_tunnel import PerTunnelMqttSink  # imports this module

            return PerTunnelMqttSink(cfg, worker_id, n_workers)
        return MqttSink(cfg, worker_id)
    if cfg.output.sink == "stdout":
        return StdoutSink()
    return DiscardSink()
