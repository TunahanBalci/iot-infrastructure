"""Entrypoint: loads config, runs the consume/produce loop, serves /healthz, /readyz and /metrics,
handles SIGTERM (stop polling, flush, commit, leave the group)."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import yaml

from .config import load_config
from .metrics import Stats, render
from .tracing import Tracing, enabled_by_env

log = logging.getLogger("telemetry_processor")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="telemetry-processor",
                                description="Validates TBMQ integration envelopes into keyed tunnel detections (Kafka)")
    p.add_argument("-c", "--config",
                   help="config YAML path (default: $PROCESSOR_CONFIG or config/telemetry-processor.yaml)")
    p.add_argument("--print-config", action="store_true",
                   help="print effective config (file + env overrides, password masked) and exit")
    p.add_argument("--benchmark", type=int, nargs="?", const=200_000, metavar="N",
                   help="measure envelopes/s of the processing path on N synthetic envelopes (no Kafka) and exit")
    return p.parse_args(argv)


def serve_http(port: int, service) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path == "/healthz":
                self._send(200, b"ok\n")
            elif self.path == "/readyz":
                ok = service.ready()
                self._send(200 if ok else 503, b"ready\n" if ok else b"not ready\n")
            elif self.path == "/metrics":
                body = render(service.stats, assigned_partitions=len(service.assigned),
                              inflight_records=service.inflight_records(), pending_batches=len(service.batches),
                              ready=service.ready())
                self._send(200, body.encode(), "text/plain; version=0.0.4")
            else:
                self._send(404, b"not found\n")

        def _send(self, code: int, body: bytes, ctype: str = "text/plain") -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # probes every few seconds: keep logs quiet
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, name="http", daemon=True).start()
    return server


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.benchmark is not None:
        from .benchmark import run_benchmark
        return run_benchmark(args.benchmark)
    try:
        cfg = load_config(args.config)
    except Exception as e:
        print(f"invalid configuration: {e}", file=sys.stderr)
        return 2
    if args.print_config:
        print(yaml.safe_dump(cfg.redacted(), sort_keys=False))
        return 0

    logging.basicConfig(level=cfg.logging.level, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from .service import Service  # confluent_kafka import after logging is set up

    k = cfg.kafka
    log.info("telemetry-processor: brokers=%s security=%s client_id=%s", k.bootstrap_servers, k.security_protocol,
             k.resolved_client_id())
    tracing = Tracing(input_topic=k.input_topic, group_id=k.group_id) if enabled_by_env() else None
    stats = Stats()
    service = Service(cfg, stats, tracing)

    def request_stop(signum, _frame):
        service.request_stop(f"received {signal.Signals(signum).name}")

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    server = serve_http(cfg.service.http_port, service) if cfg.service.http_port else None
    try:
        return service.run()
    finally:
        if tracing is not None:
            tracing.shutdown()
        if server is not None:
            server.shutdown()


if __name__ == "__main__":
    sys.exit(main())
