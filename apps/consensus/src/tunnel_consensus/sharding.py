"""Tunnel ownership. All detections of one tunnel must reach the same process, so tunnels
(not messages) are partitioned. MQTT shared subscriptions would spread one tunnel's
sensors across consumers and break association.
"""

from __future__ import annotations

import zlib

from .config import Config


def partition_of(tunnel_id: str, partitions: int) -> int:
    return zlib.crc32(tunnel_id.encode()) % partitions


class Assignment:
    def __init__(self, cfg: Config, partition: int):
        p = cfg.partitioning
        self.partitions = p.partitions
        self.partition = partition
        if not 0 <= partition < self.partitions:
            raise ValueError(f"partition {partition} outside 0..{self.partitions - 1}")
        self.cfg = cfg

    @classmethod
    def for_worker(cls, cfg: Config, ordinal: int, worker: int) -> Assignment:
        return cls(cfg, ordinal * cfg.partitioning.workers + worker)

    @property
    def sharded(self) -> bool:
        return self.partitions > 1

    def owns(self, tunnel_id: str) -> bool:
        return not self.sharded or partition_of(tunnel_id, self.partitions) == self.partition

    def tunnel_ids(self) -> list[str]:
        p = self.cfg.partitioning
        ids = (p.tunnel_id_format.format(index=p.index_offset + i) for i in range(p.tunnels))
        return [t for t in ids if self.owns(t)]

    def topic_filters(self) -> list[str]:
        """One wildcard filter when unsharded, else one filter per owned tunnel."""
        inp = self.cfg.input
        if not self.sharded:
            return [inp.topic_filter]
        return [inp.tunnel_topic_template.format(tunnel_id=t) for t in self.tunnel_ids()]

    def client_id(self) -> str:
        # Stable per partition: a restarted process resumes its persistent session.
        return f"{self.cfg.mqtt.client_id_prefix}{self.partitions}p{self.partition}"
