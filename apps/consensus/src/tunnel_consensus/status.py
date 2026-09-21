"""Shared stats names and the /healthz /readyz /metrics HTTP server."""

from __future__ import annotations

import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Engine counters (EngineStats fields + output drops) exported as consensus_<name>_total.
COUNTERS = ("received", "invalid", "foreign", "duplicates", "lost", "calibrating", "vehicles",
            "vehicles_3", "vehicles_2", "vehicles_1", "dropped", "eval_vehicles", "eval_correct", "eval_mixed",
            "replay_suppressed", "replay_dropped")
GAUGES = ("pending", "tunnels", "tunnels_calibrated", "out_pending")


def counter_lines(totals: dict) -> list[str]:
    lines = []
    for k in COUNTERS:
        lines += [f"# TYPE consensus_{k}_total counter", f"consensus_{k}_total {totals.get(k, 0)}"]
    for k in GAUGES:
        lines += [f"# TYPE consensus_{k} gauge", f"consensus_{k} {totals.get(k, 0)}"]
    return lines


def serve_http(port: int, healthy: Callable[[], bool], ready: Callable[[], bool],
               metrics: Callable[[], str]) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path == "/healthz":
                ok = healthy()
                self._send(200 if ok else 503, b"ok\n" if ok else b"unhealthy\n")
            elif self.path == "/readyz":
                ok = ready()
                self._send(200 if ok else 503, b"ready\n" if ok else b"not ready\n")
            elif self.path == "/metrics":
                self._send(200, metrics().encode(), "text/plain; version=0.0.4")
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
