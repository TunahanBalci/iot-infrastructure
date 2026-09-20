# Telemetry Processor

Sits between the TBMQ integration executor and [consensus](../consensus/README.md). TBMQ forwards
every MQTT detection to the app Kafka as an envelope with a base64 payload. This service
checks each envelope and routes it by the topic's last level:

- **valid** detections go to `iot.detections`, keyed by `tunnel_id`, so all three sensors of a
  tunnel land in the same partition.
- **device health reports** (`tunnels/+/sensors/+/health`) go to `iot.sensor-health`, same key.
  The devices report their own condition; this service only validates and forwards it.
- **everything else** goes to `iot.detections.rejected`, with a reason.

The service is stateless. Each pod runs one process with one consumer and one producer, and
KEDA scales pods on consumer lag. Delivery is at-least-once.

## Quick start

```bash
pip install -e '.[test]'
pytest                                        # unit tests, no Kafka needed
pytest -m integration -s                      # real Kafka (SASL_SSL + SCRAM) in Docker, see below
telemetry-processor --benchmark               # processing throughput on synthetic envelopes

# Against a broker
PROCESSOR__KAFKA__BOOTSTRAP_SERVERS=localhost:9092 telemetry-processor
telemetry-processor --print-config            # effective config (file + env overrides, password masked)
```

## How it works

For each record of a poll batch (up to `poll_batch` records):

1. **Envelope.** The record must be a JSON object with a string `topicName`, a strict base64
   `payload` and an integer `ts`. Anything else is rejected as `bad_envelope`.
2. **Topic.** The topic must match `tunnels/{tunnel_id}/sensors/{start|middle|end}/detections`.
   Otherwise the reason is `bad_topic`.
3. **Identity.** `clientCertCn` must be present and equal to the topic's `tunnel_id`.
   Otherwise the reason is `identity_mismatch`. The MQTT ACL already limits what a device may
   publish; this check is a second line of defense.
4. **Payload.** The decoded payload must be a UTF-8 JSON object. The checks are:
   - `schema` must be present, otherwise `bad_payload`.
   - `schema` must be the integer `2`, otherwise `unsupported_schema`.
   - Every required field must exist with the right type, otherwise `bad_payload`.
     Required: `message_id`, `sensor_id`, `tunnel_id`, `position` (str); `seq`, `ts`, `lane`,
     `occupancy_ms` (int); `speed_kmh`, `length_m`, `confidence` (number); `direction`
     (`start_to_end|end_to_start`); `classification` (one of the ten Turkish vehicle types,
     including `BILINMEYEN`); `device_id`, `device_serial`, `plate`.
   - Booleans never count as numbers, and integers beyond 64 bits are rejected.
5. **Consistency.** The payload's `tunnel_id`, `position` and `sensor_id` (`{tunnel_id}-{position}`)
   must match the topic. Otherwise the reason is `payload_mismatch`.
6. **Output.** A valid detection is written unchanged, with `received_ts` (envelope `ts`),
   `processed_ts` and `tbmq_node` added. Unknown fields, such as the simulator's `vehicle_id`
   and `true_class`, pass through untouched.

### Delivery semantics

- **Commits.** Auto-commit is off.
  - A batch's offsets can be committed only after Kafka has acknowledged every record produced
    from it, accepted or rejected.
  - Batches commit in poll order, so a committed offset never skips an unacknowledged record.
  - Commits are synchronous, at most once per `commit_interval_s`. A failed commit is retried
    with the next one.
- **Producer.** Idempotent, `acks=all`, `lz4`, `partitioner=murmur2_random` (same partition as
  the Java client for the same key), `linger.ms=5`. When the producer queue is full, the loop
  waits for acknowledgements (back-pressure).
- **Rebalance.** Assignment is cooperative-sticky.
  - On revoke, the producer is flushed and offsets are committed before the partitions move.
  - Lost partitions are never committed; their new owner reprocesses them.
- **SIGTERM.** Polling stops, the producer is flushed (up to `shutdown_timeout_s`), offsets are
  committed and the consumer leaves the group. The log ends with
  `stopped cleanly: committed offsets {...}` and exit code 0.
- **Delivery failure.** If Kafka does not acknowledge a record, the service commits the batches
  before it and exits with code 1. It never commits the failed batch, so the restarted pod
  reprocesses it.
- **Crash.** Records processed after the last commit, about 1 s worth, are produced again:
  consumers see duplicates, never gaps. Consensus deduplicates by `(sensor, boot_id, seq)`.

## Kafka interface

| Topic | Direction | Key | Content |
|---|---|---|---|
| `iot.mqtt.ingest` (48 partitions) | in, group `telemetry-processor` | none | TBMQ integration-executor envelope |
| `iot.detections` (48) | out | `tunnel_id` | detection + `received_ts`, `processed_ts`, `tbmq_node` |
| `iot.sensor-health` (48) | out | `tunnel_id` | device health report + the same three fields |
| `iot.detections.rejected` (1) | out | topic `tunnel_id`, or null before the topic is parsed | rejection |

Rejection record:

```json
{"reason": "identity_mismatch", "detail": "clientCertCn 'T000001' != topic tunnel_id 'T000042'",
 "topic": "tunnels/T000042/sensors/middle/detections", "client_cert_cn": "T000001",
 "received_ts": 1789377527990, "processed_ts": 1789377528001, "envelope": "{\"payload\": ... (max 4096 chars)"}
```

Reasons: `bad_envelope`, `bad_topic`, `identity_mismatch`, `bad_payload`, `payload_mismatch`,
`unsupported_schema`.

**ACLs.** The KafkaUser needs:

- `Read` on `iot.mqtt.ingest`
- `Read` on group `telemetry-processor`
- `Write` on `iot.detections`, `iot.sensor-health` and `iot.detections.rejected`

`Describe` is implied by these, and the idempotent producer needs no cluster ACL on Kafka ≥ 3.
At startup the service waits until it can see all four topics and logs what is missing.

## Configuration

Settings live in `config/telemetry-processor.yaml`. Every key can be overridden with
`PROCESSOR__<SECTION>__<KEY>`. Extra librdkafka properties go in `kafka.consumer` and
`kafka.producer`, for example `PROCESSOR__KAFKA__CONSUMER__SESSION_TIMEOUT_MS=15000`; `_`
becomes `.` in the property name.

In the cluster:

| Variable | Value |
|---|---|
| `PROCESSOR__KAFKA__BOOTSTRAP_SERVERS` | `app-kafka-kafka-bootstrap.iot-pipeline.svc:9093` |
| `PROCESSOR__KAFKA__SECURITY_PROTOCOL` | `SASL_SSL` |
| `PROCESSOR__KAFKA__SASL_USERNAME` | `telemetry-processor` |
| `PROCESSOR__KAFKA__SASL_PASSWORD` | Secret `telemetry-processor`, key `password` |
| `PROCESSOR__KAFKA__SSL_CA_LOCATION` | `/etc/app-kafka/ca.crt` (Secret `app-kafka-cluster-ca-cert`, key `ca.crt`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector.monitoring.svc:4318`, or empty = tracing off |
| `OTEL_SERVICE_NAME`, `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | `telemetry-processor`, `parentbased_traceidratio`, `0.01` |

Defaults already match the contract: group, topic names, port 8080, and client id
`telemetry-processor-{hostname}`.

## Observability

**Probes** (port `http`, 8080):

- `/healthz` returns 200 while the process runs.
- `/readyz` returns 200 when all of these hold:
  - The consumer has completed a rebalance and is a group member. Its assignment may be empty
    when there are more pods than partitions.
  - The producer is healthy: the topics are visible and there has been no all-brokers-down
    error since the last successful acknowledgement.
  - The service is not stopping.

**Metrics** (`/metrics`):

| Metric | Type | Meaning |
|---|---|---|
| `telemetry_processor_consumed_total` | counter | envelopes read |
| `telemetry_processor_produced_total` | counter | detections acknowledged on `iot.detections` |
| `telemetry_processor_rejected_total{reason}` | counter | envelopes rejected, all six reasons always present |
| `telemetry_processor_rejected_produced_total` | counter | rejections acknowledged on `iot.detections.rejected` |
| `telemetry_processor_commits_total` / `_commit_failures_total` | counter | offset commits |
| `telemetry_processor_consumer_errors_total` / `_delivery_failures_total` | counter | client errors |
| `telemetry_processor_rebalances_total` | counter | assign, revoke and lost callbacks |
| `telemetry_processor_traced_total` | counter | sampled records |
| `telemetry_processor_batch_duration_seconds` | histogram | poll batch validated and handed to the producer |
| `telemetry_processor_batch_ack_seconds` | histogram | poll until every record of the batch is acknowledged |
| `telemetry_processor_event_age_seconds` | histogram | `processed_ts − detection ts` of accepted detections |
| `telemetry_processor_assigned_partitions` | gauge | input partitions owned |
| `telemetry_processor_inflight_records` / `_pending_batches` | gauge | produced but unacknowledged / uncommitted |
| `telemetry_processor_ready` | gauge | same as `/readyz` |

A stats line is logged to stdout every `stats_interval_s`.

**Tracing.** Tracing is off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set and non-empty.

- Spans are exported with OTLP/HTTP.
- Each sampled record gets one span, `process iot.mqtt.ingest`, which ends when Kafka
  acknowledges the produced record. Its `traceparent` goes into the headers of that record.
- A `traceparent` header on the input record is continued. An unsampled parent is passed
  through unchanged.
- Unsampled records never touch the SDK: the ratio decision is made on a random trace id with
  the same rule as `TraceIdRatioBased`.

## Performance

Processing path, one core, synthetic 745-byte envelopes with 2% invalid
(`telemetry-processor --benchmark 300000`):

| Path | Envelopes/s |
|---|---|
| `normalize.process()` | ~255–290k |
| Batch loop, tracing off | ~235k |
| Batch loop, tracing on, 0% sampled | ~222k |
| Batch loop, tracing on, 1% sampled | ~203k |

Against Kafka 4.3 with SASL_SSL and one broker in Docker, one process (`test_integration.py`):

| Load | CPU | RSS |
|---|---|---|
| Idle | 0.002 cores | 44 MiB |
| Steady 20k envelopes/s | 0.27 cores | 49 MiB |
| Backlog drain, ~60k envelopes/s | 0.4 cores | up to ~140 MiB (librdkafka fetch and TLS buffers) |

At the drain rate the single test broker is the bottleneck, not the service. The container
image uses about 30 MiB of cgroup memory at light load.

## Integration tests

`tests/test_integration.py` needs Docker and is skipped without it. It uses
`tests/kafka-scram.sh`, which starts Kafka 4.3.1 with SASL_SSL and SCRAM-SHA-512 in the
`tp-kafka` container on `127.0.0.1:29292` (network `tp-kafka-net`), and creates the three
topics.

| Test | What it proves |
|---|---|
| end-to-end | Routing, keys, murmur2 partitions, one partition per tunnel, reject reasons, `/metrics`, `/readyz`; after SIGTERM, exit code 0 and committed offsets equal end offsets |
| SIGKILL | Killed mid-stream (100k envelopes at 20k/s) and restarted: none missing downstream (duplicates are counted) |
| rebalance | Two instances split 3/3; one gets SIGTERM mid-stream; none missing |
| tracing | OTLP/HTTP export and `traceparent` in record headers, including a continued parent |
| throughput | CPU and RSS at idle, at 20k/s and during a backlog drain |
| image | With `TP_IMAGE=telemetry-processor:test`, the image runs read-only as uid 10001 on `tp-kafka-net` |

`TP_KAFKA_KEEP=1` keeps the broker between runs.

## Limits

- **Restart delay.** A crashed pod's partitions stay idle until its group session times out
  (`session.timeout.ms`, 45 s by default). A SIGTERM'd pod hands its partitions over at once.
- **Pod count.** More pods than the 48 input partitions leaves the extras idle. They are still
  Ready.
- **Delivery failures.** A record Kafka refuses on delivery, for example because of a missing
  ACL, stops the service. The pod restarts and retries. A record refused locally, for example
  one that is too large, becomes a `bad_payload` rejection instead.
- **Field checks.** Only types are checked, not value ranges (for example `speed_kmh > 0`).
  Consensus applies its own plausibility checks.

## Layout

```
config/telemetry-processor.yaml     central configuration (every key: PROCESSOR__SECTION__KEY env override)
src/telemetry_processor/normalize.py  envelope validation, detection/rejection records (hot path)
src/telemetry_processor/service.py    consume/produce loop, commits, rebalance, shutdown
src/telemetry_processor/tracing.py    OpenTelemetry spans and trace context headers
src/telemetry_processor/metrics.py    counters, histograms, Prometheus text
src/telemetry_processor/benchmark.py  synthetic envelopes, --benchmark
src/telemetry_processor/__main__.py   entrypoint, /healthz /readyz /metrics, signals
tests/                                unit tests (fakes.py: in-memory Kafka clients), integration tests, kafka-scram.sh
```
