"""Worker process (MQTT input): one partition = one MQTT session + one consensus engine."""

from __future__ import annotations

import logging
import signal
import threading
import time
import traceback

from .config import Config
from .engine import ConsensusEngine
from .io import DiscardOutput, MqttConnection, Output, StdoutOutput
from .sharding import Assignment

log = logging.getLogger(__name__)

LOOP_INTERVAL_S = 0.1


class Worker:
    def __init__(self, cfg: Config, assignment: Assignment):
        self.cfg = cfg
        self.assignment = assignment
        self.lock = threading.Lock()  # engine is touched by paho's thread and the timer loop
        filters = assignment.topic_filters()
        self.conn = MqttConnection(cfg, assignment.client_id(), filters, self._on_payload,
                                   publish_results=cfg.output.sink == "mqtt")
        self.producer = None
        if cfg.output.sink == "kafka":
            from .kafka_io import DeliveryFailures, KafkaMetrics, KafkaSink, make_producer

            metrics = KafkaMetrics()
            self.producer = make_producer(cfg, f"{cfg.kafka.resolved_client_id()}-p{assignment.partition}",
                                          DeliveryFailures(metrics))
            self.output = KafkaSink(cfg, self.producer, metrics)
            self.kafka_metrics = metrics
            self.engine = ConsensusEngine(cfg, partition=assignment.partition, owns=assignment.owns, sink=self.output)
        else:
            self.output: Output = {"mqtt": self.conn, "stdout": StdoutOutput(),
                                   "discard": DiscardOutput()}[cfg.output.sink]
            self.engine = ConsensusEngine(cfg, self.output.publish, assignment.partition, assignment.owns)
        log.info("partition %d/%d: client_id=%s filters=%d", assignment.partition, assignment.partitions,
                 assignment.client_id(), len(filters))

    def _on_payload(self, payload: bytes) -> None:
        with self.lock:
            self.engine.ingest(payload, time.monotonic())

    def run(self, should_stop, on_stats) -> None:
        cfg = self.cfg
        h = cfg.reporting
        self.conn.start()
        deadline = time.monotonic() + cfg.mqtt.connect_timeout_s
        while not self.conn.connected_event.wait(0.2):
            if should_stop():
                return
            if time.monotonic() > deadline:
                raise ConnectionError(f"could not connect to MQTT broker {cfg.mqtt.host}:{cfg.mqtt.port} "
                                      f"within {cfg.mqtt.connect_timeout_s}s")
        now = time.monotonic()
        next_stats = now + cfg.service.stats_interval_s
        next_save = now + cfg.geometry.save_interval_s
        last_health = now
        while not should_stop():
            time.sleep(LOOP_INTERVAL_S)
            now = time.monotonic()
            with self.lock:
                if self.producer is not None:
                    self.producer.poll(0)   # delivery failure callbacks
                self.engine.tick(now)
                if h.interval_s is not None and now - last_health >= h.interval_s:
                    self.engine.report_traffic(now - last_health)
                    last_health = now
                if now >= next_save:
                    self.engine.save_geometry()
                    next_save = now + cfg.geometry.save_interval_s
            if now >= next_stats:
                on_stats(self.stats())
                next_stats = now + cfg.service.stats_interval_s

    def stats(self) -> dict:
        with self.lock:
            s = self.engine.snapshot()
        s.update(
            partition=self.assignment.partition,
            connected=self.conn.connected_event.is_set(),
            ready=self.conn.ready(),
            out_pending=self.conn.pending() if self.producer is None else len(self.producer),
            dropped=self.output.dropped if self.producer is None else self.kafka_metrics.produce_errors,
            monotonic=time.monotonic(),
        )
        return s

    def close(self, on_stats) -> None:
        self.conn.stop_receiving()
        with self.lock:
            self.engine.flush()
            self.engine.save_geometry()
        if self.producer is not None:
            remaining = self.producer.flush(self.cfg.kafka.flush_timeout_s)
            if remaining:
                log.warning("%d records not delivered at shutdown", remaining)
        on_stats(self.stats())
        self.conn.close()


def worker_main(cfg: Config, ordinal: int, worker_id: int, ready_queue, stats_queue, stop_event) -> None:
    """multiprocessing entry point."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # supervisor handles shutdown
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    logging.basicConfig(level=cfg.logging.level, format=f"%(asctime)s %(levelname)s [w{worker_id}] %(name)s: %(message)s")
    worker = None
    try:
        worker = Worker(cfg, Assignment.for_worker(cfg, ordinal, worker_id))
        ready_queue.put(("started", worker_id))
        worker.run(stop_event.is_set, stats_queue.put)
    except Exception as e:
        ready_queue.put(("error", worker_id, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"))
        raise SystemExit(1) from None
    finally:
        if worker is not None:
            try:
                worker.close(stats_queue.put)
            except Exception:  # best effort on shutdown
                log.exception("error during close")
