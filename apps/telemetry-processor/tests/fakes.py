"""In-memory stand-ins for confluent_kafka Consumer/Producer (delivery and commit timing under test control)."""

from __future__ import annotations

from types import SimpleNamespace

import orjson
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from telemetry_processor.benchmark import FakeMessage


class FakeProducer:
    def __init__(self, conf: dict):
        self.conf = conf
        self.pending: list = []    # (topic, value, key, headers, callback)
        self.delivered: list = []  # (topic, value, key, headers)
        self.buffer_errors = 0     # raise BufferError this many times
        self.refuse_topic = None   # raise KafkaException for produce() to this topic
        self.flush_delivers = True
        self.events: list = []

    def produce(self, topic, value=None, key=None, on_delivery=None, headers=None):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        if topic == self.refuse_topic:
            raise KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
        self.pending.append((topic, value, key, headers, on_delivery))

    def deliver(self, n: int | None = None, err=None, index: int | None = None) -> None:
        if index is not None:
            item = self.pending.pop(index)
            self._ack(item, err)
            return
        n = len(self.pending) if n is None else n
        items, self.pending = self.pending[:n], self.pending[n:]
        for item in items:
            self._ack(item, err)

    def _ack(self, item, err) -> None:
        topic, value, key, headers, cb = item
        if err is None:
            self.delivered.append((topic, value, key, headers))
        msg = SimpleNamespace(topic=lambda: topic, key=lambda: key)
        cb(err, msg)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=0):
        self.events.append("flush")
        if self.flush_delivers:
            self.deliver()
        return len(self.pending)

    def list_topics(self, topic=None, timeout=None):
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None)})

    def __len__(self):
        return len(self.pending)

    def records(self, topic: str) -> list[tuple]:
        return [(k, orjson.loads(v), h) for t, v, k, h in self.delivered if t == topic]


class FakeConsumer:
    def __init__(self, conf: dict, producer_events: list | None = None):
        self.conf = conf
        self.script: list = []          # batches returned by consume(); callables are invoked instead
        self.commits: list[dict] = []
        self.commit_error = None
        self.events = producer_events if producer_events is not None else []
        self.callbacks = {}
        self.closed = False

    def subscribe(self, topics, on_assign=None, on_revoke=None, on_lost=None):
        self.callbacks = {"assign": on_assign, "revoke": on_revoke, "lost": on_lost}

    def consume(self, num_messages=1, timeout=-1):
        if not self.script:
            return []
        item = self.script.pop(0)
        return item() if callable(item) else item

    def commit(self, offsets=None, asynchronous=True):
        self.events.append("commit")
        if self.commit_error is not None:
            err, self.commit_error = self.commit_error, None
            raise KafkaException(err)
        self.commits.append({tp.partition: tp.offset for tp in offsets})
        return [TopicPartition(tp.topic, tp.partition, tp.offset) for tp in offsets]

    def list_topics(self, topic=None, timeout=None):
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None)})

    def close(self):
        self.events.append("close")
        self.closed = True

    def assign(self, service, partitions):
        service._on_assign(self, [TopicPartition("iot.mqtt.ingest", p) for p in partitions])

    def revoke(self, service, partitions):
        service._on_revoke(self, [TopicPartition("iot.mqtt.ingest", p) for p in partitions])

    def lose(self, service, partitions):
        service._on_lost(self, [TopicPartition("iot.mqtt.ingest", p) for p in partitions])


def messages(values: list[bytes], partition: int = 0, start_offset: int = 0, headers=None) -> list[FakeMessage]:
    return [FakeMessage(v, partition, start_offset + i, headers) for i, v in enumerate(values)]
