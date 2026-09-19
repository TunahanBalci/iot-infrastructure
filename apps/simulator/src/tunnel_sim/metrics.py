"""Prometheus endpoint and time control of the supervisor.

The simulator runs next to the cluster (a container on the host network), so it is scraped as a static
target: http://<host>:<service.http_port>/metrics. Without it the only place the sender's own numbers
appear is the stats log line.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .clock import SimClock

log = logging.getLogger("tunnel_sim")

# metric name -> (type, help)
METRICS = {
    "tunnel_sim_up": ("gauge", "1 while the supervisor is running"),
    "tunnel_sim_published_total": ("counter", "Detections handed to the sink"),
    "tunnel_sim_dropped_total": ("counter", "Detections dropped because a connection buffer was full"),
    "tunnel_sim_missed_total": ("counter", "Detections a sensor did not report (simulated fault)"),
    "tunnel_sim_duplicates_total": ("counter", "Detections published twice (simulated fault)"),
    "tunnel_sim_vehicles_total": ("counter", "Vehicles generated"),
    "tunnel_sim_clients_connected": ("gauge", "MQTT connections currently established"),
    "tunnel_sim_clients": ("gauge", "MQTT connections the workers own"),
    "tunnel_sim_workers": ("gauge", "Worker processes started"),
    "tunnel_sim_workers_reporting": ("gauge", "Worker processes that have reported stats"),
    "tunnel_sim_workers_disconnected": ("gauge", "Worker processes with no connected client"),
    "tunnel_sim_max_lag_seconds": ("gauge", "Largest scheduler lag over the workers"),
    "tunnel_sim_pending_messages": ("gauge", "Messages buffered in the sinks"),
    "tunnel_sim_scheduled_vehicles": ("gauge", "Vehicles in flight in the simulation"),
    # Per device (labels device_id, tunnel_id, position). Only while there are few devices:
    # service.device_metrics_max keeps a load test from adding 75000 series per metric.
    "tunnel_sim_device_published_total": ("counter", "Detections published by one device"),
    "tunnel_sim_device_missed_total": ("counter", "Detections one device did not report"),
    "tunnel_sim_device_duplicates_total": ("counter", "Detections one device published twice"),
    "tunnel_sim_device_dropped_total": ("counter", "Detections of one device dropped by a full buffer"),
    "tunnel_sim_device_degraded": ("gauge", "1 while a device is in its degraded state"),
}


def render(values: dict[str, float | dict[str, float]]) -> str:
    """Prometheus text format for a snapshot.

    A value is either a number or, for the per-device metrics, a mapping of rendered label
    sets ('device_id="...",position="..."') to numbers.
    """
    out = []
    for name, (kind, help_text) in METRICS.items():
        value = values.get(name)
        if value is None:
            continue
        out += [f"# HELP {name} {help_text}", f"# TYPE {name} {kind}"]
        if isinstance(value, dict):
            out += [f"{name}{{{labels}}} {v:.6g}" for labels, v in value.items()]
        else:
            out.append(f"{name} {value:.6g}")
    return "\n".join(out) + "\n"


class MetricsServer:
    """Serves /metrics, /healthz and the time control endpoints in a daemon thread.

        curl -XPOST host:9109/time -d \'{"set": "2026-06-15T18:00:00"}\'   # jump to a time of day
        curl -XPOST host:9109/time -d \'{"offset_s": 3600}\'               # shift by an hour
        curl -XPOST host:9109/time/resync                                  # back to host time

    The offset is in memory only: restarting the simulator resyncs it with the host clock.
    """

    def __init__(self, port: int, snapshot, bind: str = "0.0.0.0", clock: SimClock | None = None):
        self.snapshot = snapshot
        self.clock = clock
        handler = self._handler()
        self._httpd = ThreadingHTTPServer((bind, port), handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, name="metrics", daemon=True)

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    def apply_time_command(self, command: dict) -> dict:
        """Apply {"set": "<ISO local time>"} or {"offset_s": <seconds>}; {} just reports."""
        clock = self.clock
        if clock is None:
            raise ValueError("time control is not available")
        if "set" in command:
            try:
                clock.set_local(datetime.fromisoformat(str(command["set"])))
            except ValueError as e:
                raise ValueError(f"set: {e}") from None
        elif "offset_s" in command:
            try:
                clock.set_offset(float(command["offset_s"]))
            except (TypeError, ValueError):
                raise ValueError("offset_s must be a number") from None
        elif command:
            raise ValueError("expected 'set' or 'offset_s'")
        return {"local": clock.local(0.0).isoformat(), "offset_s": clock.offset,
                "epoch_s": clock.epoch(0.0)}

    def _handler(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _send(self, body: bytes, content_type: str, status: int = 200):
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):  # noqa: N802 (BaseHTTPRequestHandler API)
                path = self.path.split("?")[0]
                if path == "/time":
                    self._time({})
                    return
                if path not in ("/metrics", "/healthz"):
                    self.send_error(404)
                    return
                body = (render(server.snapshot()) if path == "/metrics" else "ok\n").encode()
                self._send(body, "text/plain; version=0.0.4; charset=utf-8")

            def do_POST(self):  # noqa: N802
                path = self.path.split("?")[0]
                if path == "/time/resync":
                    self._time({"offset_s": 0})
                    return
                if path != "/time":
                    self.send_error(404)
                    return
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    command = json.loads(raw or b"{}")
                    if not isinstance(command, dict):
                        raise ValueError("body must be a JSON object")
                except ValueError as e:
                    self.send_error(400, str(e))
                    return
                self._time(command)

            def _time(self, command: dict):
                try:
                    state = server.apply_time_command(command)
                except ValueError as e:
                    self.send_error(400, str(e))
                    return
                if command:
                    log.info("simulated time is now %s (offset %+.0fs)", state["local"], state["offset_s"])
                self._send(json.dumps(state).encode(), "application/json")

            def log_message(self, *_args):  # keep scrape requests out of the log
                pass

        return Handler

    def start(self) -> None:
        self._thread.start()
        log.info("metrics on http://%s:%d/metrics", *self._httpd.server_address[:2])

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
