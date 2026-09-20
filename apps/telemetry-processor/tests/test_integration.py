"""Integration tests against a real Kafka 4.3 with SASL_SSL + SCRAM-SHA-512 (tests/kafka-scram.sh, Docker).

    pytest -m integration -s                 # starts tp-kafka on 127.0.0.1:29292, removes it afterwards
    TP_KAFKA_KEEP=1 pytest -m integration    # keep (and reuse) the Kafka container between runs
    TP_IMAGE=telemetry-processor:test pytest -m integration -k image   # also run the container image

Each scenario tags its envelopes with a unique nonce (the boot id part of message_id) and only
looks at downstream records carrying it, so a reused broker with older data is fine.
"""

from __future__ import annotations

import base64
import logging
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter, defaultdict
from pathlib import Path

import orjson
import pytest

pytestmark = pytest.mark.integration

HERE = Path(__file__).resolve().parent
APP = HERE.parent
NAME = os.environ.get("TP_KAFKA_NAME", "tp-kafka")
PORT = int(os.environ.get("TP_KAFKA_PORT", "29292"))
USER, PASSWORD = "telemetry-processor", "tp-secret"
TESTER, TESTER_PASSWORD = "tester", "tester-secret"
IN, OUT, REJ = "iot.mqtt.ingest", "iot.detections", "iot.detections.rejected"
TOPICS = ((IN, 6), (OUT, 12), (REJ, 1))
GROUP = "telemetry-processor"
POSITIONS = ("start", "middle", "end")


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


def murmur2_partition(key: bytes, partitions: int) -> int:
    """Java client DefaultPartitioner: toPositive(murmur2(key)) % partitions."""
    m, r, mask = 0x5BD1E995, 24, 0xFFFFFFFF
    length = len(key)
    h = (0x9747B28C ^ length) & mask
    for i in range(length // 4):
        k = int.from_bytes(key[i * 4:i * 4 + 4], "little")
        k = (k * m) & mask
        k ^= k >> r
        k = (k * m) & mask
        h = (h * m) & mask
        h ^= k
    tail, rest = length & ~3, length % 4
    if rest == 3:
        h ^= key[tail + 2] << 16
    if rest >= 2:
        h ^= key[tail + 1] << 8
    if rest >= 1:
        h ^= key[tail]
        h = (h * m) & mask
    h ^= h >> 13
    h = (h * m) & mask
    h ^= h >> 15
    return (h & 0x7FFFFFFF) % partitions


# --- envelopes -----------------------------------------------------------------------

def detection(nonce: str, tunnel: str, position: str, seq: int, **kw) -> dict:
    d = {"schema": 1, "message_id": f"{tunnel}-{position}:{nonce}:{seq}", "sensor_id": f"{tunnel}-{position}",
         "tunnel_id": tunnel, "position": position, "seq": seq, "ts": int(time.time() * 1000) - 50,
         "direction": "start_to_end" if seq % 2 else "end_to_start", "lane": 1 + seq % 2,
         "speed_kmh": round(60 + seq % 50 + 0.3, 1), "length_m": round(4.2 + seq % 12, 2),
         "occupancy_ms": 200 + seq % 900, "classification": "truck" if seq % 5 == 0 else "car",
         "confidence": 0.95}
    d.update(kw)
    return d


def envelope(payload: bytes | dict, tunnel: str, position: str, *, topic: str | None = None, cn: object = "=",
             **kw) -> bytes:
    raw = orjson.dumps(payload) if isinstance(payload, dict) else payload
    env = {"payload": base64.b64encode(raw).decode(), "topicName": topic or f"tunnels/{tunnel}/sensors/{position}/detections",
           "clientId": f"tunnel-sim-{tunnel}", "eventType": "PUBLISH_MSG", "qos": 1, "retain": False,
           "tbmqIeNode": "tbmq-integration-executor-0", "tbmqNode": "tbmq-0", "ts": int(time.time() * 1000),
           "clientCertCn": tunnel if cn == "=" else cn,
           "props": {"tunnel_id": tunnel, "sensor_id": f"{tunnel}-{position}", "position": position, "schema": "1"},
           "metadata": {}}
    if cn is None:
        del env["clientCertCn"]
    env.update(kw)
    return orjson.dumps(env)


def valid_stream(nonce: str, n: int, tunnels: int = 200):
    """(message_id, envelope) for n valid detections spread over tunnels x positions."""
    for i in range(n):
        tunnel = f"T{i % tunnels:06d}"
        position = POSITIONS[(i // tunnels) % 3]
        d = detection(nonce, tunnel, position, i)
        yield d["message_id"], envelope(d, tunnel, position)


def invalid_envelopes(nonce: str) -> list[tuple[str, str | None, bytes]]:
    """(expected reason, expected key, value), each carrying the nonce somewhere in the envelope text."""
    out = []
    tag = {"metadata": {"nonce": nonce}}  # payloads are base64 in the envelope text: tag the envelope itself
    for i in range(10):
        t = f"T9{i:05d}"
        good = detection(nonce, t, "middle", i)
        out += [
            ("bad_envelope", None, orjson.dumps({"topicName": f"tunnels/{t}/sensors/middle/detections",
                                                 "payload": "%%%not-base64%%%", "ts": 1, "nonce": nonce})),
            ("bad_envelope", None, f'{{"nonce": "{nonce}", truncated'.encode()),
            ("bad_topic", None, envelope(good, t, "middle", topic=f"tunnels/{t}/sensors/side/detections", **tag)),
            ("identity_mismatch", t, envelope(good, t, "middle", cn=None, **tag)),
            ("identity_mismatch", t, envelope(good, t, "middle", cn="T000001", **tag)),
            ("bad_payload", t, envelope(b"not json", t, "middle", **tag)),
            ("bad_payload", t, envelope(detection(nonce, t, "middle", i, speed_kmh="fast"), t, "middle", **tag)),
            ("payload_mismatch", t, envelope(detection(nonce, t, "end", i), t, "middle", **tag)),
            ("unsupported_schema", t, envelope(detection(nonce, t, "middle", i, schema=2), t, "middle", **tag)),
        ]
    return out


# --- Kafka helpers ------------------------------------------------------------------------

class Kafka:
    def __init__(self, bootstrap: str, ca: str):
        self.bootstrap, self.ca = bootstrap, ca

    def conf(self, **kw) -> dict:
        return {"bootstrap.servers": self.bootstrap, "security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": TESTER, "sasl.password": TESTER_PASSWORD, "ssl.ca.location": self.ca,
                "logger": logging.getLogger("kafka-test-client")} | kw

    def service_env(self, http_port: int, **kw) -> dict:
        return {"PROCESSOR__KAFKA__BOOTSTRAP_SERVERS": self.bootstrap, "PROCESSOR__KAFKA__SECURITY_PROTOCOL": "SASL_SSL",
                "PROCESSOR__KAFKA__SASL_USERNAME": USER, "PROCESSOR__KAFKA__SASL_PASSWORD": PASSWORD,
                "PROCESSOR__KAFKA__SSL_CA_LOCATION": self.ca, "PROCESSOR__SERVICE__HTTP_PORT": str(http_port),
                "PROCESSOR__SERVICE__STATS_INTERVAL_S": "2"} | kw

    def producer(self):
        from confluent_kafka import Producer
        return Producer(self.conf(**{"linger.ms": 5, "queue.buffering.max.messages": 1_000_000,
                                     "compression.type": "lz4"}))

    def produce(self, values, rate: float | None = None) -> int:
        """Keyless like the TBMQ integration executor. rate = records/s (None = as fast as possible)."""
        p = self.producer()
        n, t0 = 0, time.monotonic()
        for v in values:
            while True:
                try:
                    p.produce(IN, v)
                    break
                except BufferError:
                    p.poll(0.05)
            n += 1
            if n % 1000 == 0:
                p.poll(0)
                if rate:
                    ahead = n / rate - (time.monotonic() - t0)
                    if ahead > 0:
                        time.sleep(ahead)
        assert p.flush(60) == 0
        return n

    def high_watermarks(self, topic: str, partitions: int) -> dict[int, int]:
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer(self.conf(**{"group.id": "tp-test-reader"}))
        try:
            return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in range(partitions)}
        finally:
            c.close()

    def read(self, topic: str, partitions: int, start: dict[int, int], nonce: str) -> list[tuple]:
        """(partition, key, value dict, headers) of records after `start` whose value contains nonce."""
        from confluent_kafka import Consumer, TopicPartition
        end = self.high_watermarks(topic, partitions)
        c = Consumer(self.conf(**{"group.id": "tp-test-reader", "enable.auto.commit": False}))
        todo = {p for p in range(partitions) if end[p] > start.get(p, 0)}
        c.assign([TopicPartition(topic, p, start.get(p, 0)) for p in todo])
        out, needle = [], nonce.encode()
        deadline = time.monotonic() + 120
        while todo and time.monotonic() < deadline:
            for m in c.consume(10000, 1.0):
                if m.error():
                    continue
                if m.offset() + 1 >= end[m.partition()]:
                    todo.discard(m.partition())
                if needle in m.value():
                    out.append((m.partition(), m.key().decode() if m.key() is not None else None,
                                orjson.loads(m.value()), m.headers()))
        c.close()
        assert not todo, f"could not read {topic} to the end"
        return out

    def group_lag(self) -> int:
        from confluent_kafka import Consumer, TopicPartition
        end = self.high_watermarks(IN, 6)
        c = Consumer(self.conf(**{"group.id": GROUP, "enable.auto.commit": False}))
        try:
            committed = c.committed([TopicPartition(IN, p) for p in range(6)], timeout=10)
        finally:
            c.close()
        return sum(end[tp.partition] - max(tp.offset, 0) for tp in committed)

    def committed(self) -> dict[int, int]:
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer(self.conf(**{"group.id": GROUP, "enable.auto.commit": False}))
        try:
            return {tp.partition: tp.offset for tp in c.committed([TopicPartition(IN, p) for p in range(6)], timeout=10)}
        finally:
            c.close()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(condition, timeout: float, what: str, interval: float = 0.5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}")


def http_get(port: int, path: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except OSError:
        return 0, ""


def parse_metrics(text: str) -> dict[str, float]:
    out = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            name, value = line.rsplit(" ", 1)
            out[name] = float(value)
    return out


class Service:
    """The service from source (python -m telemetry_processor)."""

    def __init__(self, kafka: Kafka, log_dir: Path, name: str, **env):
        self.port = free_port()
        self.log_path = log_dir / f"{name}.log"
        self.log_file = self.log_path.open("w")
        environ = dict(os.environ) | kafka.service_env(self.port) | {"PYTHONUNBUFFERED": "1",
                                                                   "OTEL_EXPORTER_OTLP_ENDPOINT": ""} | env
        self.proc = subprocess.Popen([sys.executable, "-m", "telemetry_processor", "-c",
                                      str(APP / "config/telemetry-processor.yaml")],
                                     cwd=APP, env=environ, stdout=self.log_file, stderr=subprocess.STDOUT)

    def ready(self) -> bool:
        return http_get(self.port, "/readyz")[0] == 200

    def wait_ready(self, timeout: float = 60) -> None:
        wait_for(lambda: self.ready() or self.proc.poll() is not None, timeout, "service ready")
        assert self.proc.poll() is None, self.log()

    def metrics(self) -> dict[str, float]:
        code, text = http_get(self.port, "/metrics")
        assert code == 200
        return parse_metrics(text)

    def sigterm(self, timeout: float = 40) -> int:
        self.proc.send_signal(signal.SIGTERM)
        return self.proc.wait(timeout)

    def sigkill(self) -> None:
        self.proc.kill()
        self.proc.wait(10)

    def rss_mib(self) -> float:
        for line in Path(f"/proc/{self.proc.pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
        return 0.0

    def cpu_s(self) -> float:
        fields = Path(f"/proc/{self.proc.pid}/stat").read_text().rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")

    def log(self) -> str:
        self.log_file.flush()
        return self.log_path.read_text()

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(10)
        self.log_file.close()


# --- fixtures ---------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def kafka():
    if not docker_available():
        pytest.skip("docker unavailable")
    keep = os.environ.get("TP_KAFKA_KEEP") == "1"
    scram_dir = os.environ.get("KAFKA_SCRAM_DIR") or tempfile.mkdtemp(prefix="tp-kafka-")
    env = dict(os.environ, KAFKA_SCRAM_DIR=scram_dir)
    script = str(HERE / "kafka-scram.sh")
    running = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", NAME], capture_output=True, text=True)
    ca = Path(scram_dir) / NAME / "ca.crt"
    if not (keep and running.stdout.strip() == "true" and ca.exists()):
        subprocess.run([script, "up", NAME, str(PORT), f"{USER}:{PASSWORD}", f"{TESTER}:{TESTER_PASSWORD}"],
                       env=env, check=True)
    for topic, partitions in TOPICS:
        subprocess.run([script, "topic", NAME, topic, str(partitions)], env=env, check=True, capture_output=True)
    yield Kafka(f"127.0.0.1:{PORT}", str(ca))
    if not keep:
        subprocess.run([script, "down", NAME], env=env, check=False)


@pytest.fixture
def services(tmp_path):
    started: list[Service] = []

    def start(kafka: Kafka, name: str, **env) -> Service:
        s = Service(kafka, tmp_path, name, **env)
        started.append(s)
        return s

    yield start
    for s in started:
        s.stop()


def report(capsys, text: str) -> None:
    with capsys.disabled():
        print(f"\n[integration] {text}")


# --- scenarios -----------------------------------------------------------------------------------

def test_end_to_end_routing_metrics_and_clean_sigterm(kafka, services, capsys):
    nonce = uuid.uuid4().hex[:12]
    out_start, rej_start = kafka.high_watermarks(OUT, 12), kafka.high_watermarks(REJ, 1)
    valid = list(valid_stream(nonce, 3000, tunnels=50))
    invalid = invalid_envelopes(nonce)
    values = [v for _, v in valid] + [v for _, _, v in invalid]
    random.Random(1).shuffle(values)
    kafka.produce(values)

    svc = services(kafka, "e2e")
    svc.wait_ready()
    wait_for(lambda: kafka.group_lag() == 0, 60, "consumer lag 0")

    detections = kafka.read(OUT, 12, out_start, nonce)
    rejected = kafka.read(REJ, 1, rej_start, nonce)
    assert sorted(r[2]["message_id"] for r in detections) == sorted(m for m, _ in valid)  # all, no duplicates
    tunnel_partitions = defaultdict(set)
    for partition, key, value, headers in detections:
        assert key == value["tunnel_id"]
        assert partition == murmur2_partition(key.encode(), 12)  # Java-compatible murmur2
        assert {"received_ts", "processed_ts", "tbmq_node"} <= set(value) and value["tbmq_node"] == "tbmq-0"
        assert value["processed_ts"] >= value["received_ts"] and headers is None
        tunnel_partitions[key].add(partition)
    assert len(tunnel_partitions) == 50 and all(len(p) == 1 for p in tunnel_partitions.values())

    assert Counter(r[2]["reason"] for r in rejected) == Counter(reason for reason, _, _ in invalid)
    assert sorted((r[2]["reason"], r[1]) for r in rejected) == sorted((reason, key) for reason, key, _ in invalid)

    assert svc.ready()
    m = svc.metrics()
    assert m["telemetry_processor_consumed_total"] >= len(values)
    assert m["telemetry_processor_produced_total"] >= len(valid)
    for reason, n in Counter(reason for reason, _, _ in invalid).items():
        assert m[f'telemetry_processor_rejected_total{{reason="{reason}"}}'] >= n
    assert m["telemetry_processor_assigned_partitions"] == 6 and m["telemetry_processor_ready"] == 1
    assert m["telemetry_processor_commit_failures_total"] == 0
    assert m['telemetry_processor_batch_duration_seconds_bucket{le="+Inf"}'] > 0
    assert m["telemetry_processor_event_age_seconds_count"] >= len(valid)

    assert svc.sigterm() == 0
    log = svc.log()
    assert "received SIGTERM" in log and "stopped cleanly: committed offsets" in log
    committed = {p: max(o, 0) for p, o in kafka.committed().items()}  # never-used partition: no offset
    assert committed == kafka.high_watermarks(IN, 6)  # everything committed on shutdown
    report(capsys, f"e2e: {len(valid)} detections over {len(tunnel_partitions)} tunnels, "
                   f"{len(rejected)} rejections {dict(Counter(r[2]['reason'] for r in rejected))}; "
                   f"SIGTERM exit 0, committed == end offsets {committed}")


def test_at_least_once_across_sigkill(kafka, services, capsys):
    nonce = uuid.uuid4().hex[:12]
    out_start = kafka.high_watermarks(OUT, 12)
    n = 100_000
    ids = []

    def stream():
        for message_id, value in valid_stream(nonce, n):
            ids.append(message_id)
            yield value

    svc = services(kafka, "killed")
    svc.wait_ready()
    producer = threading.Thread(target=kafka.produce, args=(stream(), 20_000))
    producer.start()
    wait_for(lambda: svc.metrics()["telemetry_processor_consumed_total"] >= 30_000, 60, "30k consumed", 0.1)
    consumed_at_kill = svc.metrics()["telemetry_processor_consumed_total"]
    svc.sigkill()
    lag_after_kill = kafka.group_lag()

    restarted = services(kafka, "restarted")
    restarted.wait_ready(120)  # the killed member holds its partitions until its session times out (45 s)
    producer.join(120)
    wait_for(lambda: kafka.group_lag() == 0, 120, "consumer lag 0 after restart")
    detections = kafka.read(OUT, 12, out_start, nonce)
    seen = Counter(r[2]["message_id"] for r in detections)
    missing = set(ids) - set(seen)
    duplicates = sum(c - 1 for c in seen.values())
    assert not missing, f"{len(missing)} accepted envelopes missing downstream"
    assert restarted.sigterm() == 0
    report(capsys, f"SIGKILL after {consumed_at_kill:,.0f} consumed (group lag then {lag_after_kill:,}); "
                   f"after restart: {len(ids):,} produced, {len(seen):,} distinct downstream, 0 missing, "
                   f"{duplicates:,} duplicates")


def test_rebalance_between_two_instances_loses_nothing(kafka, services, capsys):
    nonce = uuid.uuid4().hex[:12]
    out_start = kafka.high_watermarks(OUT, 12)
    n = 150_000
    ids = []

    def stream():
        for message_id, value in valid_stream(nonce, n):
            ids.append(message_id)
            yield value

    a = services(kafka, "a")
    a.wait_ready()
    producer = threading.Thread(target=kafka.produce, args=(stream(), 10_000))
    producer.start()
    time.sleep(1.0)
    b = services(kafka, "b")
    b.wait_ready()
    wait_for(lambda: a.metrics()["telemetry_processor_assigned_partitions"] == 3
             and b.metrics()["telemetry_processor_assigned_partitions"] == 3, 60, "3 + 3 partitions")
    time.sleep(1.0)
    assert a.sigterm() == 0  # scale-in mid-stream: a commits and leaves, b takes over
    wait_for(lambda: b.metrics()["telemetry_processor_assigned_partitions"] == 6, 60, "b owns 6 partitions")
    producer.join(120)
    wait_for(lambda: kafka.group_lag() == 0, 120, "consumer lag 0")
    seen = Counter(r[2]["message_id"] for r in kafka.read(OUT, 12, out_start, nonce))
    assert not set(ids) - set(seen)
    assert "revoked partitions" in a.log() and "stopped cleanly" in a.log()
    assert "revoked partitions" in a.log() or "revoked partitions" in b.log()
    assert b.ready() and b.sigterm() == 0
    report(capsys, f"rebalance: a+b 3/3 partitions, a SIGTERM mid-stream -> b 6; {n:,} produced, 0 missing, "
                   f"{sum(c - 1 for c in seen.values()):,} duplicates")


def test_throughput_and_resources(kafka, services, capsys):
    """Sustained 20k envelopes/s (CPU/RSS at target), then a backlog drain (max throughput of one process)."""
    nonce = uuid.uuid4().hex[:12]
    rate, seconds = 20_000, 15
    svc = services(kafka, "perf")
    svc.wait_ready()
    time.sleep(3)
    cpu0, t0 = svc.cpu_s(), time.monotonic()
    time.sleep(5)
    idle_cpu = (svc.cpu_s() - cpu0) / (time.monotonic() - t0)
    idle_rss = svc.rss_mib()

    producer = threading.Thread(target=kafka.produce,
                                args=((v for _, v in valid_stream(nonce, rate * seconds, 25_000)), rate))
    producer.start()
    time.sleep(3)  # steady state
    samples = []
    m0, cpu0, t0 = svc.metrics()["telemetry_processor_consumed_total"], svc.cpu_s(), time.monotonic()
    while producer.is_alive() and time.monotonic() - t0 < seconds - 5:
        time.sleep(0.5)
        samples.append(svc.rss_mib())
    m1, cpu1, t1 = svc.metrics()["telemetry_processor_consumed_total"], svc.cpu_s(), time.monotonic()
    steady_rate, steady_cpu, steady_rss = (m1 - m0) / (t1 - t0), (cpu1 - cpu0) / (t1 - t0), max(samples)
    producer.join(120)
    wait_for(lambda: kafka.group_lag() == 0, 60, "lag 0")
    ages = svc.metrics()
    assert svc.sigterm() == 0

    # Backlog drain: pre-produce, then start a fresh process and measure its steady consumption rate.
    backlog = 400_000
    kafka.produce(v for _, v in valid_stream(uuid.uuid4().hex[:12], backlog, 25_000))
    drain = services(kafka, "drain")
    points = []
    wait_for(lambda: drain.ready() or drain.proc.poll() is not None, 60, "drain ready", 0.1)
    while True:
        m = drain.metrics()
        points.append((time.monotonic(), m["telemetry_processor_consumed_total"], drain.cpu_s(), drain.rss_mib()))
        if m["telemetry_processor_consumed_total"] >= backlog:
            break
        assert time.monotonic() - points[0][0] < 180, "drain too slow"
        time.sleep(0.5)
    lo = next(p for p in points if p[1] >= 0.1 * backlog)
    hi = next(p for p in points if p[1] >= 0.9 * backlog)
    drain_rate = (hi[1] - lo[1]) / (hi[0] - lo[0])
    drain_cpu = (hi[2] - lo[2]) / (hi[0] - lo[0])
    drain_rss = max(p[3] for p in points)
    wait_for(lambda: kafka.group_lag() == 0, 60, "lag 0 after drain")
    assert drain.sigterm() == 0
    age_count = ages["telemetry_processor_event_age_seconds_count"]
    age_p1s = ages['telemetry_processor_event_age_seconds_bucket{le="0.25"}'] / age_count
    report(capsys, f"idle: rss {idle_rss:.0f} MiB, cpu {idle_cpu:.3f} cores | "
                   f"steady {steady_rate:,.0f}/s: cpu {steady_cpu:.2f} cores, peak rss {steady_rss:.0f} MiB, "
                   f"event age <=250ms {age_p1s:.1%} | backlog drain {drain_rate:,.0f}/s: cpu {drain_cpu:.2f} cores, "
                   f"peak rss {drain_rss:.0f} MiB")
    assert steady_rate > 0.9 * rate and drain_rate >= 20_000


def test_tracing_exports_otlp_and_propagates_traceparent(kafka, services, capsys):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    posts = []

    class Collector(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            posts.append((self.path, self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    collector = ThreadingHTTPServer(("127.0.0.1", 0), Collector)
    threading.Thread(target=collector.serve_forever, daemon=True).start()
    nonce = uuid.uuid4().hex[:12]
    out_start = kafka.high_watermarks(OUT, 12)
    n = 4000
    parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    p = kafka.producer()
    for i, (_, value) in enumerate(valid_stream(nonce, n, 40)):
        p.produce(IN, value, headers=[("traceparent", parent.encode())] if i == 0 else None)
    assert p.flush(30) == 0
    svc = services(kafka, "traced", OTEL_EXPORTER_OTLP_ENDPOINT=f"http://127.0.0.1:{collector.server_port}",
                   OTEL_SERVICE_NAME="telemetry-processor", OTEL_TRACES_SAMPLER="parentbased_traceidratio",
                   OTEL_TRACES_SAMPLER_ARG="0.01", OTEL_BSP_SCHEDULE_DELAY="500")
    svc.wait_ready()
    wait_for(lambda: kafka.group_lag() == 0, 60, "lag 0")
    assert svc.sigterm() == 0  # shutdown exports pending spans
    collector.shutdown()
    records = kafka.read(OUT, 12, out_start, nonce)
    traced = [dict(h) for _, _, _, h in records if h]
    continued = [h for h in traced if h["traceparent"].startswith(parent[:36].encode())]
    assert len(records) == n and len(continued) == 1
    assert 0 < len(traced) < 0.03 * n  # ~1% sampled roots + the continued parent
    assert posts and all(path == "/v1/traces" for path, _ in posts)
    assert b"telemetry-processor" in b"".join(body for _, body in posts)
    report(capsys, f"tracing: {len(traced)}/{n} records carry a traceparent (1 continued from the input header), "
                   f"{len(posts)} OTLP/HTTP export(s) to /v1/traces")


@pytest.mark.skipif(not os.environ.get("TP_IMAGE"), reason="set TP_IMAGE=<image> to test the container image")
def test_image_against_kafka(kafka, capsys, tmp_path):
    nonce = uuid.uuid4().hex[:12]
    out_start = kafka.high_watermarks(OUT, 12)
    valid = list(valid_stream(nonce, 2000, tunnels=20))
    kafka.produce(v for _, v in valid)
    port, name = free_port(), f"tp-image-{nonce}"
    cmd = ["docker", "run", "-d", "--name", name, "--network", f"{NAME}-net", "--read-only", "--memory", "256m",
           "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "-p", f"127.0.0.1:{port}:8080",
           "-v", f"{kafka.ca}:/etc/app-kafka/ca.crt:ro",
           "-e", f"PROCESSOR__KAFKA__BOOTSTRAP_SERVERS={NAME}:9093", "-e", "PROCESSOR__KAFKA__SECURITY_PROTOCOL=SASL_SSL",
           "-e", f"PROCESSOR__KAFKA__SASL_USERNAME={USER}", "-e", f"PROCESSOR__KAFKA__SASL_PASSWORD={PASSWORD}",
           "-e", "PROCESSOR__KAFKA__SSL_CA_LOCATION=/etc/app-kafka/ca.crt", "-e", "OTEL_EXPORTER_OTLP_ENDPOINT=",
           os.environ["TP_IMAGE"]]
    subprocess.run(cmd, check=True, capture_output=True)
    try:
        wait_for(lambda: http_get(port, "/readyz")[0] == 200, 60, "container ready")
        wait_for(lambda: kafka.group_lag() == 0, 60, "lag 0")
        seen = {r[2]["message_id"] for r in kafka.read(OUT, 12, out_start, nonce)}
        assert seen == {m for m, _ in valid}
        metrics = parse_metrics(http_get(port, "/metrics")[1])
        user = subprocess.run(["docker", "inspect", "-f", "{{.Config.User}}", name], capture_output=True, text=True)
        stats = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", name],
                               capture_output=True, text=True).stdout.strip()
        subprocess.run(["docker", "stop", "-t", "30", name], check=True, capture_output=True)
        exit_code = subprocess.run(["docker", "inspect", "-f", "{{.State.ExitCode}}", name],
                                   capture_output=True, text=True).stdout.strip()
        logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True).stdout
        assert exit_code == "0" and "stopped cleanly" in logs, logs[-2000:]
        report(capsys, f"image {os.environ['TP_IMAGE']} (user {user.stdout.strip()}, read-only rootfs): "
                       f"{len(seen)} detections, ready, consumed_total={metrics['telemetry_processor_consumed_total']:.0f}, "
                       f"mem {stats}, docker stop -> exit {exit_code}")
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
