"""Counters and histograms (plain attributes, updated by the main loop) and Prometheus text rendering."""

from __future__ import annotations

from bisect import bisect_left

from .normalize import REASONS

PREFIX = "telemetry_processor_"
BATCH_BUCKETS_S = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
ACK_BUCKETS_S = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
EVENT_AGE_BUCKETS_S = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 1800.0, 3600.0)


class Histogram:
    __slots__ = ("bounds", "counts", "sum")

    def __init__(self, bounds: tuple[float, ...]):
        self.bounds = bounds
        self.counts = [0] * (len(bounds) + 1)  # last slot = +Inf
        self.sum = 0.0

    def observe(self, value: float) -> None:
        self.counts[bisect_left(self.bounds, value)] += 1  # bucket le=b holds values <= b
        self.sum += value

    def render(self, name: str, help_text: str) -> list[str]:
        counts = list(self.counts)  # snapshot: buckets and _count stay consistent
        lines = [f"# HELP {name} {help_text}", f"# TYPE {name} histogram"]
        cumulative = 0
        for bound, n in zip(self.bounds, counts):
            cumulative += n
            lines.append(f'{name}_bucket{{le="{bound:g}"}} {cumulative}')
        cumulative += counts[-1]
        lines += [f'{name}_bucket{{le="+Inf"}} {cumulative}', f"{name}_sum {self.sum:.6f}", f"{name}_count {cumulative}"]
        return lines


class Stats:
    """Counters owned by the main loop. The HTTP thread only reads them."""

    def __init__(self) -> None:
        self.consumed = 0              # envelopes read from the input topic
        self.health = 0                # device health reports routed to the health topic
        self.produced = 0              # detections acknowledged on the output topic
        self.rejected = dict.fromkeys(REASONS, 0)   # envelopes rejected, by reason
        self.rejected_produced = 0     # rejections acknowledged on the rejected topic
        self.commits = 0
        self.commit_failures = 0
        self.consumer_errors = 0
        self.delivery_failures = 0
        self.rebalances = 0
        self.traced = 0                # sampled records (spans started)
        self.batch_duration = Histogram(BATCH_BUCKETS_S)   # poll return -> last record handed to the producer
        self.batch_ack = Histogram(ACK_BUCKETS_S)          # poll return -> every record of the batch acknowledged
        self.event_age = Histogram(EVENT_AGE_BUCKETS_S)    # processed_ts - detection ts (accepted records)

    def rejected_total(self) -> int:
        return sum(self.rejected.values())


def render(stats: Stats, *, assigned_partitions: int, inflight_records: int, pending_batches: int,
           ready: bool) -> str:
    p = PREFIX
    lines: list[str] = []

    def counter(name: str, help_text: str, value: int) -> None:
        lines.extend((f"# HELP {p}{name} {help_text}", f"# TYPE {p}{name} counter", f"{p}{name} {value}"))

    def gauge(name: str, help_text: str, value: int) -> None:
        lines.extend((f"# HELP {p}{name} {help_text}", f"# TYPE {p}{name} gauge", f"{p}{name} {value}"))

    counter("consumed_total", "Envelopes consumed from the input topic.", stats.consumed)
    counter("produced_total", "Detections acknowledged by Kafka on the output topic.", stats.produced)
    counter("health_total", "Device health reports routed to the health topic.", stats.health)
    lines += [f"# HELP {p}rejected_total Envelopes rejected, by reason.", f"# TYPE {p}rejected_total counter"]
    lines += [f'{p}rejected_total{{reason="{r}"}} {n}' for r, n in stats.rejected.items()]
    counter("rejected_produced_total", "Rejections acknowledged by Kafka on the rejected topic.",
            stats.rejected_produced)
    counter("commits_total", "Successful offset commits.", stats.commits)
    counter("commit_failures_total", "Failed offset commits (retried with the next commit).", stats.commit_failures)
    counter("consumer_errors_total", "Error events returned by the consumer.", stats.consumer_errors)
    counter("delivery_failures_total", "Records Kafka did not acknowledge (the service stops, offsets stay uncommitted).",
            stats.delivery_failures)
    counter("rebalances_total", "Partition assignment changes (assign, revoke, lost).", stats.rebalances)
    counter("traced_total", "Records traced (sampled spans).", stats.traced)
    lines += stats.batch_duration.render(f"{p}batch_duration_seconds",
                                         "Time to validate a poll batch and hand it to the producer.")
    lines += stats.batch_ack.render(f"{p}batch_ack_seconds",
                                    "Time from poll until every record of the batch is acknowledged.")
    lines += stats.event_age.render(f"{p}event_age_seconds",
                                    "processed_ts minus detection ts of accepted detections.")
    gauge("assigned_partitions", "Input partitions assigned to this consumer.", assigned_partitions)
    gauge("inflight_records", "Records handed to the producer and not yet acknowledged.", inflight_records)
    gauge("pending_batches", "Poll batches waiting for acknowledgements before their offsets can be committed.",
          pending_batches)
    gauge("ready", "1 when the consumer is in the group and the producer is healthy.", int(ready))
    return "\n".join(lines) + "\n"
