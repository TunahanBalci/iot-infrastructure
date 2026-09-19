"""mqtt.connection_mode: per_tunnel — config, tunnel/client mapping, and the I/O loop against a fake broker."""

import random
import socket
import threading
import time

import pytest

from tunnel_sim.__main__ import main
from tunnel_sim.config import Config, apply_env_overrides, load_config
from tunnel_sim.per_tunnel import PerTunnelMqttSink, reconnect_delay
from tunnel_sim.sinks import Sink, make_sink
from tunnel_sim.worker import Worker


def per_tunnel_config(tunnels: int = 6, **mqtt) -> Config:
    return Config.model_validate({"topology": {"tunnels": tunnels},
                                  "mqtt": {"connection_mode": "per_tunnel", **mqtt}})


class OfflineSink(PerTunnelMqttSink):
    """Per-tunnel sink that never connects."""

    def start(self):
        pass


def wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.01)


# --- config --------------------------------------------------------------

def test_connection_mode_defaults_to_shared_and_env_selects_per_tunnel():
    assert Config().mqtt.connection_mode == "shared"
    cfg = load_config("config/simulator.yaml", environ={"SIM__MQTT__CONNECTION_MODE": "per_tunnel"})
    assert cfg.mqtt.connection_mode == "per_tunnel"
    sink = make_sink(cfg, worker_id=0, n_workers=2)
    assert isinstance(sink, PerTunnelMqttSink)  # connects only in start()
    sink.close()
    with pytest.raises(ValueError):
        Config.model_validate({"mqtt": {"connection_mode": "per_sensor"}})


@pytest.mark.parametrize("raw, expected", [
    ("/certs/{tunnel_id}.pem", "/certs/{tunnel_id}.pem"),
    ("{tunnel_id}.pem", "{tunnel_id}.pem"),              # YAML parse error if parsed as YAML
    ("{tunnel_id}", "{tunnel_id}"),                      # YAML flow mapping if parsed as YAML
    ('"/certs/{tunnel_id}.pem"', "/certs/{tunnel_id}.pem"),
])
def test_env_override_keeps_brace_templates_as_strings(raw, expected):
    data = apply_env_overrides({}, {"SIM__MQTT__CONNECTION_MODE": "per_tunnel", "SIM__MQTT__TLS__CERTFILE": raw,
                                    "SIM__MQTT__TLS__KEYFILE": "/certs/{tunnel_id}.key", "SIM__MQTT__PORT": "8883"})
    cfg = Config.model_validate(data)
    assert cfg.mqtt.tls.certfile == expected
    assert cfg.mqtt.port == 8883
    assert cfg.mqtt.tls.files_for("T000123") == (expected.format(tunnel_id="T000123"), "/certs/T000123.key")


def test_tunnel_id_placeholder_requires_per_tunnel_mode():
    for key in ("certfile", "keyfile"):
        with pytest.raises(ValueError, match="per_tunnel"):
            Config.model_validate({"mqtt": {"tls": {key: "/certs/{tunnel_id}.pem"}}})
    with pytest.raises(ValueError, match="allowed: tunnel_id"):
        per_tunnel_config(tls={"certfile": "/certs/{tunnel}.pem"})
    Config.model_validate({"mqtt": {"tls": {"certfile": "/certs/client.pem", "keyfile": "/certs/client.key"}}})
    per_tunnel_config(tls={"certfile": "/certs/shared.pem"})  # one certificate for all tunnels is allowed


def test_cli_exits_2_on_placeholder_in_shared_mode(monkeypatch, capsys):
    monkeypatch.setenv("SIM__MQTT__TLS__CERTFILE", "/certs/{tunnel_id}.pem")
    assert main(["--config", "config/simulator.yaml", "--print-config"]) == 2
    monkeypatch.setenv("SIM__MQTT__CONNECTION_MODE", "per_tunnel")
    assert main(["--config", "config/simulator.yaml", "--print-config"]) == 0
    assert "certfile: /certs/{tunnel_id}.pem" in capsys.readouterr().out


# --- tunnel -> client mapping -----------------------------------------------

def test_one_client_per_tunnel_with_ids_topics_and_cert_paths():
    cfg = per_tunnel_config(tunnels=7, client_id_prefix="tunnel-sim", connections_per_worker=4,
                            tls={"enabled": True, "ca_certs": "/certs/ca.pem",
                                 "certfile": "/certs/{tunnel_id}.pem", "keyfile": "/certs/{tunnel_id}.key"})
    seen = []
    for worker_id in range(2):
        sink = OfflineSink(cfg, worker_id, n_workers=2)
        w = Worker(cfg, worker_id, 2, "x", sink=sink)
        assert sink.clients() == (0, len(w.tunnels))  # connections_per_worker is ignored
        for t in w.tunnels:
            tc = sink.clients_by_tunnel[t.tunnel_id]
            assert {s.sink_ref[0] for s in t.sensors} == {tc}
            assert tc.client_id == f"tunnel-sim-{t.tunnel_id}"
            assert tc.status_topic == f"simulator/tunnel-sim-{t.tunnel_id}/status"
            assert tc.client.will_topic == tc.status_topic
            assert (tc.certfile, tc.keyfile) == (f"/certs/{t.tunnel_id}.pem", f"/certs/{t.tunnel_id}.key")
            seen.append(t.tunnel_id)
    assert sorted(seen) == [f"T{i:06d}" for i in range(7)]


def test_backpressure_is_per_worker_across_clients():
    cfg = per_tunnel_config(tunnels=3, max_pending_messages=5)
    sink = OfflineSink(cfg, 0)
    w = Worker(cfg, 0, 1, "x", sink=sink)
    sensors = [s for t in w.tunnels for s in t.sensors]
    assert not sink.publish(sensors[0], b"{}")  # not connected yet
    for tc in sink.clients_by_tunnel.values():
        tc.connected = True
    accepted = [sink.publish(sensors[i % len(sensors)], b"{}") for i in range(8)]
    assert accepted == [True] * 5 + [False] * 3
    assert sink.pending() == 5


def test_reconnect_delay_honours_bounds_and_is_jittered():
    rng = random.Random(1)
    for failures in range(1, 20):
        delays = [reconnect_delay(failures, (1, 30), rng) for _ in range(200)]
        assert all(1 <= d <= 30 for d in delays)
        assert max(delays) - min(delays) > 0.5  # not synchronised
    assert max(reconnect_delay(1, (1, 30), rng) for _ in range(200)) <= 2
    assert reconnect_delay(3, (5, 5), rng) == 5


def test_worker_stats_include_clients_and_sink_drops():
    class Counting(Sink):
        def publish(self, sensor, payload):
            return True

        def clients(self):
            return 2, 3

        def dropped(self):
            return 7

    snapshots = []
    w = Worker(per_tunnel_config(tunnels=2), 0, 1, "x", sink=Counting())
    w.stats.dropped = 1
    w._emit_stats(snapshots.append)
    assert (snapshots[0]["dropped"], snapshots[0]["clients_connected"], snapshots[0]["clients"]) == (8, 2, 3)


# --- I/O loop against a fake broker -----------------------------------------------

def _varint(buf: bytes, pos: int) -> tuple[int, int]:
    value, mult = 0, 1
    while True:
        b = buf[pos]
        pos += 1
        value += (b & 127) * mult
        mult *= 128
        if not b & 128:
            return value, pos


class FakeBroker:
    """Just enough MQTT 5 to accept connections and record what each client id publishes."""

    def __init__(self):
        self.server = socket.create_server(("127.0.0.1", 0))
        self.port = self.server.getsockname()[1]
        self.lock = threading.Lock()
        self.refuse: set[str] = set()  # client ids answered with CONNACK "Not authorized"
        self.connects: list[str] = []
        self.publishes: list[tuple[str, str, bytes]] = []
        self.conns: dict[str, socket.socket] = {}
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                conn, _ = self.server.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket):
        f, client_id = conn.makefile("rb"), None
        try:
            while header := f.read(1):
                mult, length = 1, 0
                while True:
                    b = f.read(1)[0]
                    length += (b & 127) * mult
                    mult *= 128
                    if not b & 128:
                        break
                body, kind = f.read(length), header[0] >> 4
                if kind == 1:  # CONNECT: protocol name, version, flags, keepalive, properties, client id
                    props_len, pos = _varint(body, 10)
                    pos += props_len
                    client_id = body[pos + 2:pos + 2 + int.from_bytes(body[pos:pos + 2])].decode()
                    with self.lock:
                        self.connects.append(client_id)
                        self.conns[client_id] = conn
                    if client_id in self.refuse:
                        conn.sendall(b"\x20\x03\x00\x87\x00")
                        break
                    conn.sendall(b"\x20\x03\x00\x00\x00")
                elif kind == 3:  # PUBLISH
                    pos = 2 + int.from_bytes(body[:2])
                    topic = body[2:pos].decode()
                    if header[0] >> 1 & 3:
                        conn.sendall(b"\x40\x02" + body[pos:pos + 2])
                        pos += 2
                    props_len, pos = _varint(body, pos)
                    with self.lock:
                        self.publishes.append((client_id, topic, body[pos + props_len:]))
                elif kind == 12:  # PINGREQ
                    conn.sendall(b"\xd0\x00")
                elif kind == 14:  # DISCONNECT
                    break
        except (OSError, IndexError):
            pass
        finally:
            conn.close()

    def kick(self, client_id: str) -> None:
        self.conns[client_id].shutdown(socket.SHUT_RDWR)

    def received(self, topic_prefix: str) -> list[tuple[str, str, bytes]]:
        with self.lock:
            return [p for p in self.publishes if p[1].startswith(topic_prefix)]

    def close(self):
        self.server.close()


@pytest.fixture
def broker():
    b = FakeBroker()
    yield b
    b.close()


def test_per_tunnel_clients_publish_over_their_own_connection(broker):
    cfg = per_tunnel_config(tunnels=6, host="127.0.0.1", port=broker.port, connect_timeout_s=5,
                            reconnect_delay_s=[1, 1])
    sink = PerTunnelMqttSink(cfg, 0, n_workers=2)
    w = Worker(cfg, 0, 2, "x", sink=sink)  # start() connects
    try:
        assert sink.clients() == (3, 3)
        assert sorted(broker.connects) == ["tunnel-sim-T000000", "tunnel-sim-T000002", "tunnel-sim-T000004"]
        wait_for(lambda: len(broker.received("simulator/")) == 3)
        assert {(c, t, p) for c, t, p in broker.received("simulator/")} == {
            (c, f"simulator/{c}/status", b"online") for c in broker.connects}

        for t in w.tunnels:
            for s in t.sensors:
                assert sink.publish(s, b'{"n":1}')
        wait_for(lambda: len(broker.received("tunnels/")) == 9)
        for client_id, topic, payload in broker.received("tunnels/"):
            assert client_id == f"tunnel-sim-{topic.split('/')[1]}" and payload == b'{"n":1}'

        broker.kick("tunnel-sim-T000002")
        wait_for(lambda: sink.clients()[0] == 2)
        assert not sink.publish(w.tunnels[1].sensors[0], b"{}")  # dropped while disconnected
        wait_for(lambda: sink.clients() == (3, 3) and broker.connects.count("tunnel-sim-T000002") == 2)
        assert sink.disconnects == 1
    finally:
        w.close()
    wait_for(lambda: len(broker.received("simulator/")) == 3 + 1 + 3)  # online, online after reconnect, offline
    assert sorted(c for c, _, p in broker.received("simulator/") if p == b"offline") == sorted(set(broker.connects))
    assert sink.clients() == (0, 3)


def test_start_continues_when_some_clients_cannot_connect(broker):
    broker.refuse.add("tunnel-sim-T000001")
    cfg = per_tunnel_config(tunnels=3, host="127.0.0.1", port=broker.port, connect_timeout_s=0.5,
                            reconnect_delay_s=[1, 1])
    sink = PerTunnelMqttSink(cfg, 0)
    w = Worker(cfg, 0, 1, "x", sink=sink)
    try:
        assert sink.clients() == (2, 3)
        assert sink.connect_failures >= 1 and "Not authorized" in sink.last_error
    finally:
        w.close()


def test_start_fails_if_no_client_connects_within_timeout():
    with socket.create_server(("127.0.0.1", 0)) as s:
        port = s.getsockname()[1]
    cfg = per_tunnel_config(tunnels=4, host="127.0.0.1", port=port, connect_timeout_s=0.5)
    started = time.monotonic()
    with pytest.raises(ConnectionError, match="could not connect any of 4 clients"):
        Worker(cfg, 0, 1, "x")
    assert time.monotonic() - started < 3

