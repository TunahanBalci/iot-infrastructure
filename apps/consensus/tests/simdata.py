"""Detections from the simulator model in simulated time (no broker), with ground truth."""

from __future__ import annotations

import heapq

EPOCH_S = 1_800_000_000.0


def simulate(seconds: float, tunnels: int = 12, index_offset: int = 0, boot_id: str = "b00t01", **sensors) -> list[bytes]:
    from tunnel_sim.config import Config as SimConfig
    from tunnel_sim.sinks import Sink
    from tunnel_sim.worker import Worker as SimWorker

    class CaptureSink(Sink):
        def __init__(self):
            self.payloads = []

        def publish(self, sensor, payload):
            self.payloads.append(payload)
            return True

    cfg = SimConfig.model_validate({
        "topology": {"tunnels": tunnels, "index_offset": index_offset},
        "payload": {"include_vehicle_id": True, "include_true_class": True},
        "sensors": sensors,
    })
    sink = CaptureSink()
    w = SimWorker(cfg, worker_id=0, n_workers=1, boot_id=boot_id, sink=sink)
    w.encoder.epoch_offset_s = EPOCH_S
    w.seed_arrivals()
    w.fast_forward()
    while w.heap and w.heap[0][0] <= seconds:
        w._handle(heapq.heappop(w.heap), live=True)
    return sink.payloads
