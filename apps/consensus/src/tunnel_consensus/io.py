"""MQTT 5.0 connection (subscribe detections + publish results) and debug sinks."""

from __future__ import annotations

import logging
import ssl
import sys
import threading
from collections.abc import Callable

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .config import Config
from .model import SCHEMA_VERSION

log = logging.getLogger(__name__)


class PrepackedProperties:
    """PUBLISH properties packed once and reused (paho re-encodes Properties on every publish)."""

    __slots__ = ("_packed", "_repr")

    def __init__(self, props: Properties):
        self._packed = props.pack()
        self._repr = str(props)

    def pack(self) -> bytes:
        return self._packed

    def __str__(self) -> str:
        return self._repr


class Output:
    """Where fused results go. publish(topic, payload, retain)."""

    dropped = 0

    def publish(self, topic: str, payload: bytes, retain: bool) -> None:
        raise NotImplementedError


class DiscardOutput(Output):
    def publish(self, topic: str, payload: bytes, retain: bool) -> None:
        pass


class StdoutOutput(Output):
    def __init__(self) -> None:
        self._out = sys.stdout.buffer
        self._lock = threading.Lock()

    def publish(self, topic: str, payload: bytes, retain: bool) -> None:
        with self._lock:
            self._out.write(topic.encode() + b" " + payload + b"\n")


class MqttConnection(Output):
    """One MQTT 5.0 session per partition. Detections arrive on paho's network thread and
    are handed to `on_payload`; results are published on the same connection."""

    def __init__(self, cfg: Config, client_id: str, topic_filters: list[str], on_payload: Callable[[bytes], None],
                 publish_results: bool = True):
        m, o = cfg.mqtt, cfg.output
        self.client_id = client_id
        self.filters = topic_filters
        self.batch = cfg.input.subscribe_batch
        self.sub_qos = cfg.input.qos
        self.out_qos = o.qos
        self.max_pending = m.max_pending_messages
        self.publish_results = publish_results
        self.on_payload = on_payload
        self.status_topic = m.status_topic_template.format(client_id=client_id)
        self.connected_event = threading.Event()
        self.subscribed_event = threading.Event()
        self.closing = False
        self.dropped = 0
        self.sub_failures = 0
        self._pending_subs: set[int] = set()
        self._lock = threading.Lock()

        props = Properties(PacketTypes.PUBLISH)
        props.PayloadFormatIndicator = 1
        props.ContentType = "application/json"
        if o.message_expiry_s:
            props.MessageExpiryInterval = o.message_expiry_s
        props.UserProperty = [("producer", "tunnel-consensus"), ("schema", str(SCHEMA_VERSION))]
        self.props = PrepackedProperties(props)

        self.client = c = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv5,
            transport=m.transport,
        )
        if m.username is not None:
            c.username_pw_set(m.username, m.password)
        if m.tls.enabled:
            c.tls_set(ca_certs=m.tls.ca_certs, certfile=m.tls.certfile, keyfile=m.tls.keyfile,
                      cert_reqs=ssl.CERT_REQUIRED if not m.tls.insecure else ssl.CERT_NONE)
            c.tls_insecure_set(m.tls.insecure)
        c.reconnect_delay_set(*m.reconnect_delay_s)
        will_props = Properties(PacketTypes.WILLMESSAGE)
        will_props.ContentType = "text/plain"
        c.will_set(self.status_topic, b"offline", qos=1, retain=True, properties=will_props)
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_subscribe = self._on_subscribe
        c.on_message = self._on_message

        connect_props = Properties(PacketTypes.CONNECT)
        connect_props.SessionExpiryInterval = m.session_expiry_s
        c.connect_async(m.host, m.port, keepalive=m.keepalive_s, clean_start=m.clean_start, properties=connect_props)

    def start(self) -> None:
        self.client.loop_start()

    # --- callbacks (paho network thread) ------------------------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("%s: connect refused: %s (TBMQ: check APPLICATION credentials)", self.client_id, reason_code)
            return
        log.info("%s: connected (session_present=%s), subscribing to %d filter(s)",
                 self.client_id, flags.session_present, len(self.filters))
        client.publish(self.status_topic, b"online", qos=1, retain=True)
        self.connected_event.set()
        # Always (re)subscribe: idempotent, and covers a session that expired while away.
        self.subscribed_event.clear()
        with self._lock:
            self._pending_subs.clear()
            self.sub_failures = 0
            for i in range(0, len(self.filters), self.batch):
                chunk = [(f, mqtt.SubscribeOptions(qos=self.sub_qos)) for f in self.filters[i:i + self.batch]]
                rc, mid = client.subscribe(chunk)
                if rc != mqtt.MQTT_ERR_SUCCESS:
                    log.error("%s: subscribe failed rc=%s", self.client_id, rc)
                    return
                self._pending_subs.add(mid)

    def _on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        failed = [rc for rc in reason_code_list if rc.is_failure]
        if failed:
            self.sub_failures += len(failed)
            log.error("%s: %d subscription(s) refused: %s (TBMQ: check subscribe auth rules)",
                      self.client_id, len(failed), failed[0])
        with self._lock:
            self._pending_subs.discard(mid)
            if not self._pending_subs and not self.sub_failures:
                log.info("%s: subscribed", self.client_id)
                self.subscribed_event.set()

    def _on_message(self, client, userdata, msg):
        self.on_payload(msg.payload)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected_event.clear()
        self.subscribed_event.clear()
        if self.closing:
            log.debug("%s: disconnected", self.client_id)
        else:
            log.warning("%s: disconnected: %s (reconnecting)", self.client_id, reason_code)

    # --- output -----------------------------------------------------------

    def publish(self, topic: str, payload: bytes, retain: bool) -> None:
        if not self.publish_results:
            return
        if self.pending() >= self.max_pending:
            self.dropped += 1
            return
        info = self.client.publish(topic, payload, self.out_qos, retain, properties=self.props)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            self.dropped += 1

    def pending(self) -> int:
        # paho has no public accessor for the outgoing packet buffer; len() of a deque is atomic.
        out = getattr(self.client, "_out_packet", None)
        return len(out) if out is not None else 0

    def ready(self) -> bool:
        return self.connected_event.is_set() and self.subscribed_event.is_set()

    def stop_receiving(self) -> None:
        """Unsubscribe-free stop: detach the handler so no detection is processed after flush."""
        self.on_payload = lambda _payload: None

    def close(self) -> None:
        c = self.client
        self.closing = True
        if self.connected_event.is_set():
            c.publish(self.status_topic, b"offline", qos=1, retain=True).wait_for_publish(timeout=5)
        c.disconnect()
        c.loop_stop()
