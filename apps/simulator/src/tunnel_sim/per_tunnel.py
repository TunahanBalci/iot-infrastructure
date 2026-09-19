"""connection_mode: per_tunnel — one MQTT client (and client certificate) per tunnel.

A thread per client does not scale to thousands of tunnels, so a worker uses a fixed set of threads:

- the simulation thread only appends publishes to a queue and never calls paho;
- one I/O thread owns every client whose socket is open. It drains the queue and multiplexes
  reads, writes and keepalives of all clients through a selector
  (socket / want_write / loop_read / loop_write / loop_misc);
- CONNECT_THREADS connector threads run the blocking part of a connection attempt (TCP connect,
  TLS handshake, CONNECT), bounded by paho's connect timeout and the keepalive, so a slow
  handshake never stalls the I/O loop.

A client belongs to exactly one of these threads at a time, so paho is never used concurrently.
Connection attempts are paced (mqtt.connect_rate_per_s, split across workers) and retried with
jittered exponential backoff, so neither startup nor a broker restart causes a connect storm.
"""

from __future__ import annotations

import heapq
import itertools
import logging
import queue
import random
import selectors
import socket
import threading
import time
from collections import deque

import paho.mqtt.client as mqtt

from .config import Config
from .model import Sensor
from .sinks import Sink, configure_tls, connect_properties, create_client, out_queue_len, publish_properties

log = logging.getLogger(__name__)

CONNECT_THREADS = 4           # concurrent blocking connection attempts per worker
MISC_INTERVAL_S = 1.0         # keepalive pings and CONNACK / ping timeouts
HEALTH_LOG_INTERVAL_S = 10.0  # disconnects and failed attempts are summarised instead of logged per client
CLOSE_TIMEOUT_S = 5.0
MAX_SELECT_S = 0.5
MAX_READS_PER_EVENT = 64      # TLS can hold decrypted records the selector does not report
CONNECT_BURST_S = 0.05        # unused connect pacing credit is kept for at most this long
READ_WRITE = selectors.EVENT_READ | selectors.EVENT_WRITE


def reconnect_delay(failures: int, delay_range: tuple[float, float], rng: random.Random | None = None) -> float:
    """Seconds before the next attempt after `failures` consecutive failures (>= 1).

    Full jitter between min and an exponentially growing ceiling capped at max: clients that
    lost their connection at the same moment do not come back at the same moment.
    """
    lo, hi = delay_range
    ceiling = min(hi, max(lo, 1) * 2 ** min(failures, 16))
    return (rng or random).uniform(lo, max(lo, ceiling))


class TunnelClient:
    """One tunnel's MQTT connection; used by all three of its sensors."""

    __slots__ = ("tunnel_id", "client_id", "status_topic", "certfile", "keyfile", "client", "tls_ready",
                 "sock", "fd", "events", "buffered", "connected", "ever_connected", "failures", "last_error")

    def __init__(self, cfg: Config, tunnel_id: str):
        m = cfg.mqtt
        self.tunnel_id = tunnel_id
        self.client_id = f"{m.client_id_prefix}-{tunnel_id}"
        self.status_topic = m.status_topic_template.format(client_id=self.client_id)
        self.certfile, self.keyfile = m.tls.files_for(tunnel_id)
        self.client = create_client(m, self.client_id, self.status_topic)
        self.client.user_data_set(self)
        self.tls_ready = not m.tls.enabled  # certificates are loaded by the first connection attempt
        self.sock: object = None            # set while the I/O thread owns an open socket
        self.fd = -1
        self.events = 0
        self.buffered = 0                   # packets in paho's out buffer, as last counted
        self.connected = False              # CONNACK received; read by the simulation thread
        self.ever_connected = False
        self.failures = 0
        self.last_error = ""


class PerTunnelMqttSink(Sink):
    def __init__(self, cfg: Config, worker_id: int, n_workers: int = 1):
        m = cfg.mqtt
        self.cfg = cfg
        self.qos = m.qos
        self.retain = m.retain
        self.max_pending = m.max_pending_messages  # per worker, all clients together
        self.connect_interval_s = n_workers / m.connect_rate_per_s
        self.clients_by_tunnel: dict[str, TunnelClient] = {}
        # Written by the I/O thread only.
        self.n_connected = 0
        self.first_connects = 0
        self.disconnects = 0
        self.connect_failures = 0
        self.last_error = ""
        self._late_dropped = 0
        self._buffered = 0
        self._open: set[TunnelClient] = set()
        self._due: list[tuple[float, int, TunnelClient]] = []
        self._due_seq = itertools.count()
        self._in_flight = 0
        self._next_connect_at = 0.0
        # Cross-thread hand-offs.
        self._queue: deque[tuple] = deque()     # simulation -> I/O
        self._attempts: queue.SimpleQueue = queue.SimpleQueue()  # I/O -> connectors
        self._attempted: deque[tuple[TunnelClient, str | None]] = deque()  # connectors -> I/O
        self._idle = False
        self._closing = False
        self._closed = False
        self._io_error: BaseException | None = None
        self._sel = selectors.DefaultSelector()
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_w.setblocking(False)
        self._sel.register(self._wake_r, selectors.EVENT_READ, None)
        self._io_thread = threading.Thread(target=self._io_main, name=f"mqtt-io-w{worker_id}", daemon=True)
        self._connectors = [threading.Thread(target=self._connector_main, name=f"mqtt-connect-w{worker_id}-{i}",
                                             daemon=True) for i in range(CONNECT_THREADS)]

    # --- Sink interface (simulation thread) --------------------------------

    def attach(self, sensor: Sensor) -> None:
        tc = self.clients_by_tunnel.get(sensor.tunnel_id)
        if tc is None:
            tc = self.clients_by_tunnel[sensor.tunnel_id] = TunnelClient(self.cfg, sensor.tunnel_id)
            tc.client.on_connect = self._on_connect
            tc.client.on_disconnect = self._on_disconnect
        sensor.sink_ref = (tc, publish_properties(self.cfg.mqtt, sensor))

    def start(self) -> None:
        """Connect all clients. Returns once all connected at least once, or once none newly connected
        for connect_timeout_s (the rest keep retrying); raises ConnectionError if none connected at all."""
        m = self.cfg.mqtt
        n = len(self.clients_by_tunnel)
        if n == 0:
            return
        _raise_open_files_limit(n + 256)
        now = time.monotonic()
        for tc in self.clients_by_tunnel.values():
            tc.client.max_queued_messages_set(max(1, self.max_pending // n))  # QoS>0 messages awaiting ack
            tc.client.connect_async(m.host, m.port, keepalive=m.keepalive_s, clean_start=True,
                                    properties=connect_properties(m))
            self._schedule(tc, now)
        self._next_connect_at = now + random.uniform(0, self.connect_interval_s)
        for t in self._connectors:
            t.start()
        self._io_thread.start()

        seen, progress_at = 0, now
        while self.first_connects < n:
            time.sleep(0.1)
            if self._io_error is not None:
                raise RuntimeError("MQTT I/O thread failed") from self._io_error
            if self.first_connects != seen:
                seen, progress_at = self.first_connects, time.monotonic()
            elif time.monotonic() - progress_at >= m.connect_timeout_s:
                if seen == 0:
                    error = self.last_error
                    self.close()
                    raise ConnectionError(f"could not connect any of {n} clients to MQTT broker {m.host}:{m.port} "
                                          f"within {m.connect_timeout_s}s (last error: {error or 'none'})")
                log.warning("%d/%d MQTT clients connected, none newly connected for %.0fs; starting anyway, "
                            "the rest keep retrying (last error: %s)", self.n_connected, n, m.connect_timeout_s,
                            self.last_error or "none")
                break
        log.info("%d/%d MQTT clients connected in %.1fs", self.n_connected, n, time.monotonic() - now)

    def publish(self, sensor: Sensor, payload: bytes) -> bool:
        tc, props = sensor.sink_ref  # type: ignore[misc]
        if self._io_error is not None:
            raise RuntimeError("MQTT I/O thread failed") from self._io_error
        if not tc.connected or len(self._queue) + self._buffered >= self.max_pending:
            return False
        self._queue.append((tc, sensor.topic, payload, props, self.retain))
        if self._idle:  # the I/O thread may be blocked in select()
            self._idle = False
            self._wake()
        return True

    def publish_retained(self, sensor: Sensor, topic: str, payload: bytes) -> bool:
        tc, props = sensor.sink_ref  # type: ignore[misc]
        if not tc.connected:
            return False
        self._queue.append((tc, topic, payload, props, True))
        if self._idle:
            self._idle = False
            self._wake()
        return True

    def pending(self) -> int:
        return len(self._queue) + self._buffered

    def connected(self) -> bool:
        return self.n_connected == len(self.clients_by_tunnel)

    def clients(self) -> tuple[int, int]:
        return self.n_connected, len(self.clients_by_tunnel)

    def dropped(self) -> int:
        return self._late_dropped

    def close(self) -> None:
        if self._closed:
            return
        self._closed = self._closing = True
        if self._io_thread.is_alive():
            self._wake()
            self._io_thread.join(CLOSE_TIMEOUT_S + 2)
        for t in self._connectors:
            if t.is_alive():
                self._attempts.put(None)
        if not self._io_thread.is_alive():
            self._sel.close()
            self._wake_w.close()
            self._wake_r.close()

    def _wake(self) -> None:
        try:
            self._wake_w.send(b"\0")
        except OSError:  # buffer full (a wake-up is already pending) or closed
            pass

    # --- connector threads ------------------------------------------------

    def _connector_main(self) -> None:
        tls = self.cfg.mqtt.tls
        while (tc := self._attempts.get()) is not None:
            error = None
            try:
                if self._closing:
                    raise ConnectionAbortedError("sink closing")
                if not tc.tls_ready:
                    configure_tls(tc.client, tls, tc.certfile, tc.keyfile)
                    tc.tls_ready = True
                tc.client.reconnect()  # blocking connect + handshake; queues and writes CONNECT
                if tc.client.socket() is None:
                    error = "connection closed while sending CONNECT"
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
            self._attempted.append((tc, error))
            self._wake()

    # --- I/O thread -------------------------------------------------------

    def _io_main(self) -> None:
        try:
            self._io_loop()
        except BaseException as e:
            log.exception("MQTT I/O thread failed")
            self._io_error = e

    def _io_loop(self) -> None:
        sel = self._sel
        now = time.monotonic()
        next_misc, next_health = now + MISC_INTERVAL_S, now + HEALTH_LOG_INTERVAL_S
        logged = (0, 0)
        close_deadline = None
        while True:
            now = time.monotonic()
            if self._closing and close_deadline is None:
                close_deadline = now + CLOSE_TIMEOUT_S
                self._drain()
                self._disconnect_all()
            if close_deadline is not None and (now >= close_deadline or not (self._open or self._in_flight)):
                break
            while self._attempted:
                self._adopt(*self._attempted.popleft(), now)
            if close_deadline is None:
                self._start_attempts(now)
                self._drain()
            if now >= next_misc:
                for tc in list(self._open):
                    tc.client.loop_misc()
                    self._sync(tc)
                next_misc = now + MISC_INTERVAL_S
            if now >= next_health:
                if (self.disconnects, self.connect_failures) != logged:
                    log.warning("MQTT clients connected %d/%d; last %.0fs: %d disconnects, %d failed connection "
                                "attempts (last error: %s)", self.n_connected, len(self.clients_by_tunnel),
                                HEALTH_LOG_INTERVAL_S, self.disconnects - logged[0],
                                self.connect_failures - logged[1], self.last_error)
                    logged = (self.disconnects, self.connect_failures)
                next_health = now + HEALTH_LOG_INTERVAL_S

            timeout = min(next_misc - now, MAX_SELECT_S)
            if self._due and self._in_flight < CONNECT_THREADS and close_deadline is None:
                timeout = min(timeout, max(self._due[0][0], self._next_connect_at) - now)
            self._idle = True
            if self._queue or self._attempted or (self._closing and close_deadline is None):
                timeout = 0
            events = sel.select(max(timeout, 0))
            self._idle = False
            for key, mask in events:
                if key.data is None:
                    try:
                        self._wake_r.recv(4096)
                    except OSError:
                        pass
                else:
                    self._on_ready(key.data, mask)

        for tc in list(self._open):  # did not finish closing in time
            sel.unregister(tc.fd)
            tc.sock.close()  # type: ignore[attr-defined]
        self._open.clear()

    def _schedule(self, tc: TunnelClient, due: float) -> None:
        heapq.heappush(self._due, (due, next(self._due_seq), tc))

    def _start_attempts(self, now: float) -> None:
        due, interval = self._due, self.connect_interval_s
        while due and due[0][0] <= now and self._in_flight < CONNECT_THREADS and self._next_connect_at <= now:
            self._in_flight += 1
            self._attempts.put(heapq.heappop(due)[2])
            self._next_connect_at = max(self._next_connect_at, now - CONNECT_BURST_S) + interval

    def _adopt(self, tc: TunnelClient, error: str | None, now: float) -> None:
        """Take over a client from a connector thread."""
        self._in_flight -= 1
        if error is not None:
            self._attempt_failed(tc, error, now)
            return
        tc.last_error = ""
        tc.sock = tc.client.socket()
        tc.fd = tc.sock.fileno()  # type: ignore[attr-defined]
        tc.events = selectors.EVENT_READ
        self._sel.register(tc.fd, tc.events, tc)
        self._open.add(tc)
        if self._closing:
            tc.client.disconnect()
        self._sync(tc)

    def _attempt_failed(self, tc: TunnelClient, error: str, now: float) -> None:
        self.connect_failures += 1
        self.last_error = f"{tc.client_id}: {error}"
        log.debug("%s: connection attempt failed: %s", tc.client_id, error)
        if not self._closing:
            tc.failures += 1
            self._schedule(tc, now + reconnect_delay(tc.failures, self.cfg.mqtt.reconnect_delay_s))

    def _drain(self) -> None:
        q, qos = self._queue, self.qos
        for _ in range(len(q)):
            tc, topic, payload, props, retain = q.popleft()
            if tc.sock is None or not tc.connected:  # connection lost after publish() accepted it
                self._late_dropped += 1
                continue
            # Retained health messages go out at QoS 1: a health report is worth a round trip.
            if tc.client.publish(topic, payload, 1 if retain else qos, retain,
                                 properties=props).rc != mqtt.MQTT_ERR_SUCCESS:
                self._late_dropped += 1
            self._sync(tc)

    def _on_ready(self, tc: TunnelClient, mask: int) -> None:
        c, sock = tc.client, tc.sock
        if sock is None:
            return
        if mask & selectors.EVENT_READ:
            for _ in range(MAX_READS_PER_EVENT):
                c.loop_read()
                if c.socket() is not sock or not _tls_buffered(sock):
                    break
        if mask & selectors.EVENT_WRITE and c.socket() is sock:
            c.loop_write()
        self._sync(tc)

    def _sync(self, tc: TunnelClient) -> None:
        """Reconcile bookkeeping after any paho call on an open client."""
        c = tc.client
        if c.socket() is not tc.sock:
            self._on_closed(tc)
            return
        n = out_queue_len(c)
        if n != tc.buffered:
            self._buffered += n - tc.buffered
            tc.buffered = n
        events = READ_WRITE if n else selectors.EVENT_READ
        if events != tc.events:
            self._sel.modify(tc.fd, events, tc)
            tc.events = events

    def _on_closed(self, tc: TunnelClient) -> None:
        """paho closed the socket (error, broker disconnect, keepalive timeout or our own DISCONNECT)."""
        self._sel.unregister(tc.fd)
        self._open.discard(tc)
        self._buffered -= tc.buffered
        tc.sock, tc.fd, tc.events, tc.buffered = None, -1, 0, 0
        was_connected, tc.connected = tc.connected, False
        if was_connected:
            self.n_connected -= 1
        if self._closing:
            return
        now = time.monotonic()
        if not was_connected:
            self._attempt_failed(tc, tc.last_error or "connection closed before CONNACK", now)
            return
        self.disconnects += 1
        self.last_error = f"{tc.client_id}: {tc.last_error or 'connection lost'}"
        log.debug("%s: %s", tc.client_id, tc.last_error or "connection lost")
        self._schedule(tc, now + reconnect_delay(1, self.cfg.mqtt.reconnect_delay_s))

    def _disconnect_all(self) -> None:
        self._due.clear()
        for tc in list(self._open):
            if tc.connected:
                tc.client.publish(tc.status_topic, b"offline", qos=1, retain=True)
            tc.client.disconnect()
            self._sync(tc)

    # paho callbacks, invoked by loop_read/loop_misc on the I/O thread (on_disconnect also from reconnect).

    def _on_connect(self, client, tc: TunnelClient, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            tc.last_error = f"connect refused: {reason_code}"
            return
        tc.connected = True
        tc.failures = 0
        self.n_connected += 1
        if not tc.ever_connected:
            tc.ever_connected = True
            self.first_connects += 1
        log.debug("%s: connected", tc.client_id)
        client.publish(tc.status_topic, b"online", qos=1, retain=True)

    def _on_disconnect(self, client, tc: TunnelClient, flags, reason_code, properties) -> None:
        # Keep a CONNACK refusal reason rather than the generic error paho reports right after it.
        if flags.is_disconnect_packet_from_server:
            tc.last_error = f"disconnected by broker: {reason_code}"
        elif tc.connected:
            tc.last_error = f"connection lost ({reason_code})"


def _tls_buffered(sock) -> int:
    pending = getattr(sock, "pending", None)
    return pending() if pending is not None else 0


def _raise_open_files_limit(needed: int) -> None:
    """Each client holds a socket; the common default soft limit of 1024 is too low."""
    try:
        import resource
    except ImportError:  # non-Unix
        return
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft == resource.RLIM_INFINITY or soft >= needed:
        return
    target = needed if hard == resource.RLIM_INFINITY else min(hard, needed)
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    if target < needed:
        log.warning("open files limit %d is below the %d needed for one connection per tunnel", target, needed)
