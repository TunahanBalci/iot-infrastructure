# Tunnel Consensus Service

Downstream of the [simulator](../simulator/README.md). Each tunnel has three sensors
(start, middle, end) that report every vehicle, with misses, duplicates, noise and
misclassifications. This service fuses the detections of one vehicle into **one vehicle
event** with a consensus vehicle type. It also publishes per-tunnel traffic counts.
(Per-device health is reported by the devices themselves; see the
[simulator](../simulator/README.md).)

Two modes (`input.source`):

- **`mqtt`** (default): TBMQ APPLICATION client, tunnels partitioned over worker processes.
- **`kafka`**: member of the consumer group `consensus` on `iot.detections`, outputs to
  `iot.vehicles` and `iot.traffic`, scaled by KEDA. See
  [Kafka mode](#kafka-mode).

No per-tunnel configuration is needed: sensor spacing and measurement noise are learned from
the traffic. Detections that read the same plate are the same vehicle; where a plate is
missing, association falls back to the timing geometry.

## Quick start

```bash
pip install -e '.[test]' -e ../simulator    # simulator only needed for the end-to-end tests
pytest                                       # unit + end-to-end quality tests, no broker needed
pytest -m integration -s tests/integration   # Kafka mode against Kafka in Docker (~4 min)

# Against a broker
CONSENSUS__MQTT__HOST=localhost tunnel-consensus
# Against Kafka (see "Kafka mode")
CONSENSUS__INPUT__SOURCE=kafka CONSENSUS__OUTPUT__SINK=kafka CONSENSUS__KAFKA__BOOTSTRAP_SERVERS=localhost:9093 \
  CONSENSUS__KAFKA__SASL_PASSWORD=... CONSENSUS__KAFKA__SSL_CA_LOCATION=ca.crt tunnel-consensus
tunnel-consensus --print-config              # effective config (file + env overrides)
tunnel-consensus --print-assignment          # partitions, client ids, subscriptions
```

**On the cluster only Kafka mode is deployed.** `make install-processing` (step 80, part of
`make install`) builds the image, imports it into k3s containerd — the tag is a hash of this
directory's contents, so the step is skipped when nothing changed — and applies a Deployment plus a
KEDA `ScaledObject` in `iot-pipeline` (see [Scaling with KEDA](#scaling-with-keda)). MQTT mode is
still supported by the code and the tests, but nothing deploys it; step 80 deletes the old
`iot-consensus` namespace if it finds one.

Related targets:

| Command | What it does |
|---|---|
| `make verify-processing` | Checks readiness and throughput of both pipeline services |
| `make logs-consensus` | Follows the service logs |
| `make watch-topic TOPIC=iot.vehicles` | Prints vehicle events live |
| `make consensus-test` | Unit + simulator end-to-end tests locally |

The settings are the `CONSENSUS_*` variables in `config.env`.

## How it works

For each detection:

1. **Dedup.** Drop repeats of `(sensor, boot_id, seq)`. Gaps in `seq` are counted as lost
   messages. Missed vehicles do not use up a `seq`, so a gap means the message was lost
   in transit.
2. **Geometry.** Sensors are L/2 apart. A pair of detections from adjacent sensors at t1
   and t2, with speed v, implies `L/2 = (t2 − t1) · v`. Every plausible pair of long
   vehicles votes in a log-scale histogram:
   - Pairs from the same vehicle agree to within the speed noise and pile up in one bin.
   - Unrelated pairs spread out.
   - Votes are weighted by how closely the two measured speeds and lengths match.

   Calibration takes about 1–3 minutes of traffic per tunnel. The estimate keeps being
   refined from fused vehicles and is saved to `geometry.state_dir` (MQTT) or to the
   compacted `iot.consensus.geometry` topic (Kafka).
3. **Pend.** Detections at the first and middle sensor (in travel order) wait, per
   direction and lane.
4. **Associate at the last sensor.** At constant speed, the middle detection sits at the
   midpoint of the first and last:
   `t_middle ≈ (t_first + t_last) / 2`.
   - This constraint only carries timestamp jitter, not speed error, so it tells vehicles
     apart even in dense lanes.
   - Candidate triplets are scored chi-square-style: midpoint residual, each measured
     speed against the timing speed, and length spread. Each term is normalized by the
     sensor's learned noise.
   - If no triplet fits, the best consistent pair is used. Otherwise the detection is
     emitted on its own.
5. **Expire.** When a pending detection's vehicle should have passed the last sensor
   (event-time watermark + `max_lateness_ms`), the last sensor missed it. The detection
   then pairs with a pending partner or is emitted alone.
6. **Fuse.** Classification is a log-odds vote: `Σ w_position · log(c · (K − 1) / (1 − c))`.
   - A random flip comes with low confidence, so it barely counts.
   - `w_position` is the static `fusion.position_weights` (empty by default, so every sensor
     weighs 1.0). Nothing here scores a sensor's reliability — the devices report their own
     condition on `tunnels/+/sensors/+/health`. What *is* learned per sensor is speed and
     length noise, used by the association gates in step 4.
   - Speed comes from the travel time between the outermost detections, not from any
     single sensor.

### Quality

Measured by `tests/test_end_to_end.py`: the simulator model runs in simulated time with
ground truth, 12 tunnels, 15 minutes.

| Scenario | Vehicles fused perfectly¹ | Mixed events | Classification accuracy |
|---|---|---|---|
| Simulator defaults (5% degraded sensors) | ~98% | ~2% | ~99.6% (one sensor alone: ~95%) |
| 30% degraded sensors (5× error rates) | ~83% | ~6% | ~98.8% |

¹ Exactly one event containing all of that vehicle's detections.

Engine throughput is about 80–90k detections/s on one core, including JSON parsing and
excluding MQTT.

## MQTT interface

**Input.** Subscribes to `tunnels/+/sensors/+/detections`, simulator schema 2.

**Vehicle events.** Published to `tunnels/{tunnel_id}/vehicles`:

```json
{
  "schema": 2,
  "event_id": "T000042-end:9f3a1c:1234",
  "tunnel_id": "T000042",
  "direction": "end_to_start",
  "lane": 1,
  "ts_entry": 1789377527984,
  "ts_exit": 1789377611020,
  "plate": "14 ABC 123",
  "speed_kmh": 71.8,
  "length_m": 15.87,
  "classification": "KAMYON",
  "confidence": 1.0,
  "agreement": 1.0,
  "sensors": 3,
  "missing": [],
  "detections": ["T000042-end:9f3a1c:1234", "T000042-middle:9f3a1c:1190", "T000042-start:9f3a1c:1301"]
}
```

| Field | Meaning |
|---|---|
| `event_id` | `message_id` of the first detection in travel order. Deterministic. |
| `ts_entry`, `ts_exit` | Time at the first and last sensor. Estimated from the travel speed when that sensor missed. |
| `plate` | Plate of the vehicle, from the detections that read one. |
| `classification` | Consensus vehicle type: each sensor votes for what it reported, weighted by the log-odds of its confidence; `BILINMEYEN` abstains. |
| `agreement` | Share of fused detections that match the consensus type. |
| `sensors`, `missing` | How many sensors contributed, and which positions missed. |
| `vehicle_id`, `true_class` | Passed through only when the simulator includes ground truth. |

**Traffic.** Published retained to `tunnels/{tunnel_id}/traffic` every `reporting.interval_s`.
Contains the learned `length_m`, `counts` (direction → vehicle type → count) and
`avg_speed_kmh`.

**Device health** is not published here. Each device reports its own condition on
`tunnels/{tunnel_id}/sensors/{position}/health`, which the telemetry-processor routes to
`iot.sensor-health`.

**Status.** A retained `online`/`offline` message plus the MQTT 5 Will go to
`consensus/{client_id}/status`.

## Kafka mode

```bash
CONSENSUS__INPUT__SOURCE=kafka CONSENSUS__OUTPUT__SINK=kafka \
CONSENSUS__KAFKA__SASL_PASSWORD=... tunnel-consensus      # defaults: contract bootstrap, SCRAM user consensus,
                                                          # CA /etc/app-kafka/ca.crt, contract topic names
```

One process per pod, no worker processes (`partitioning.*` is ignored). Settings are under
`kafka:` in [config/consensus.yaml](config/consensus.yaml); librdkafka properties can be
passed through `kafka.consumer` / `kafka.producer`.

### Topics and records

| Direction | Topic | Key | Value |
|---|---|---|---|
| in | `iot.detections` | tunnel_id | detection, simulator schema 2 (extra fields ignored) |
| out | `iot.vehicles` | tunnel_id | vehicle event, as on MQTT |
| out | `iot.traffic` | tunnel_id | traffic report + `ts` (report time, ms) |
| in/out | `iot.consensus.geometry` (compacted) | tunnel_id | `{"tunnel_id", "length_m", "updated_ts"}` |

Producer: `acks=all`, idempotent, lz4, `murmur2_random` partitioner. Reports are plain
records (no retained semantics); MQTT reports also carry `ts`.

### Tunnel ownership

Detections are keyed by tunnel_id with the Java murmur2 partitioner, so a tunnel's three
sensors always land in one partition. Each assigned partition gets its own engine with its
own event clock (one lagging partition cannot expire another partition's vehicles). The
assignment strategy is `cooperative-sticky`: a rebalance only moves the partitions it has to,
the others keep their state.

### Delivery semantics: at least once, no partial events

- **Commit.** Every `kafka.commit_interval_s` the producer is flushed, then each partition
  commits the offset of the oldest detection still pending in its engine (vehicle not
  emitted yet), lowered to the first detection of any emitted vehicle that straddles that
  point. A delivery failure stops the process without committing past it (exit 1).
- **Commit metadata** records the oldest pending offset, the position and the event clock.
- **Revoke** (rebalance, SIGTERM): write changed geometry, flush, commit, then drop the
  partition's state **without** emitting partial events.
- **Replay.** The new owner reads from the committed offset. Vehicles finalized before the
  previous owner's position are fused again to rebuild state but not emitted. Pending
  detections never expire early during the replay. When it reaches that position, detections
  whose deadline had clearly passed the previous owner's clock are dropped: that owner
  emitted them. Everything else continues normally.
- **Crash** (SIGKILL, OOM): same as revoke from the last commit. Events emitted after that
  commit are emitted again with the **same `event_id`**; storage deduplicates by `event_id`.
- **Geometry.** The geometry topic is read to its end at startup and before new partitions
  are taken over, and seeds their tunnels. Nothing recalibrates after a rebalance. Lengths
  are written when a tunnel calibrates (before the next commit), every
  `geometry.save_interval_s` for owned tunnels whose length changed, and on revoke/shutdown.

Measured by `tests/integration/test_kafka_rebalance.py`: 50 tunnels, 15 simulated minutes,
174k detections, two members, one SIGKILLed then restarted, one stopped with SIGTERM
(4 handovers). Results against a baseline member that was never interrupted:

| | Baseline | With 4 handovers |
|---|---|---|
| Vehicle events | 58,560 | 58,872 |
| Vehicles lost (baseline vehicles missing) | – | 0 of 38,949 |
| Extra events with a duplicate `event_id` | – | 303 (mostly re-emitted after the SIGKILL) |
| Vehicles fused differently (other `event_id`) | – | 15 (0.04%) |
| Vehicles fused perfectly / accuracy | 96.68% / 99.53% | 96.64% / 99.53% |
| Tunnels calibrated | 50 | 50, each once; the restarted member calibrated none |
| SIGTERM to exit | – | 0.6 s |

Throughput (`tests/integration/bench_kafka.py`, pre-filled topic, SASL_SSL, output to Kafka):
72k records/s with 400 tunnels and 68k/s with 1000 tunnels, on one core. That includes
librdkafka's threads, so it matches the engine alone. Peak RSS while catching up 2.4M records
of 1000 tunnels was 364 MiB with `association.dedup_window: 128`.

### HTTP and metrics

- `/healthz`: the main loop turned within 120 s and no fatal error occurred.
- `/readyz`: joined the group (first assignment done, possibly with zero partitions),
  geometry loaded, not stopping.
- `/metrics`: the MQTT-mode series (`consensus_received_total`, `consensus_vehicles_total`, ...)
  plus:

| Metric | Meaning |
|---|---|
| `consensus_kafka_assigned_partitions` | partitions owned by this pod |
| `consensus_kafka_replaying_partitions` | partitions still replaying after a takeover |
| `consensus_kafka_lag_records` | records behind the partition ends |
| `consensus_kafka_uncommitted_records` | records a handover of this pod's partitions would replay |
| `consensus_kafka_rebalances_total`, `consensus_kafka_lost_partitions_total` | rebalances; partitions lost to a session timeout |
| `consensus_kafka_commits_total`, `consensus_kafka_commit_failures_total` | commits; failed commits or flushes |
| `consensus_kafka_produce_errors_total`, `consensus_kafka_produce_backpressure_total` | failed deliveries (fatal); producer queue full |
| `consensus_kafka_revoked_pending_total` | pending detections dropped on revoke (the new owner re-reads them) |
| `consensus_replay_suppressed_total`, `consensus_replay_dropped_total` | vehicles re-fused but not re-emitted; detections dropped at the end of a replay |
| `consensus_geometry_loaded_tunnels`, `consensus_geometry_records_{read,published}_total` | geometry topic |
| `consensus_event_age_seconds` | histogram: emit time − `ts_exit` |

### Tracing

On only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set and non-empty (OTLP/HTTP). If a
detection record carries a **sampled** W3C `traceparent` header, the vehicle event fused from
it gets a `consensus fuse` span, a child of that detection with links to the other sampled
ones. The span context is written to the vehicle record's `traceparent` header. Unsampled
records cost nothing beyond a header lookup. Sampling follows `OTEL_TRACES_SAMPLER`.

### Scaling with KEDA

- **Replicas.** At most one per partition of `iot.detections` (48). Replicas beyond that sit
  idle.
- **Scaling cost.** Every scale-out or scale-in moves partitions, and the new owner replays
  from the committed offset. That is roughly the longest vehicle travel time in the partition
  (see `consensus_kafka_uncommitted_records`) at about 70k records/s per core, after which it
  catches up. Use a stabilization window of several minutes.
- **Scale on `consensus_kafka_lag_records`, not on committed-offset lag.** The committed
  offset trails the position on purpose, by the oldest pending vehicle: typically 5–10 minutes
  of data (slow trucks in long tunnels, longer when a sensor missed). A Kafka-lag trigger would
  count that as lag and stay at max replicas. Use a Prometheus trigger on
  `sum(consensus_kafka_lag_records)`: records not yet consumed by any member.
- **Graceful stop.** SIGTERM stops in well under a second (flush + commit + leave group).
- **Memory.** It grows with the pending detections: vehicles inside the owned tunnels.

### Limits

- **Duplicates.** At least once: after a crash, events emitted since the last commit are
  emitted again with the same `event_id`.
- **Fusion right after a handover.** The new owner starts without the learned sensor noise.
  In dense single-lane traffic with degraded sensors, a few vehicles near
  the handover point are fused differently than an uninterrupted run would (different
  `event_id`, never missing): 0.04% of vehicles in the test above.
- **Duplicate copies at the replay start.** A duplicate message whose original lies before
  the committed offset is treated as a new detection.
- **First report after a takeover.** The traffic window starts when the replay ends: the moved
  tunnels report no traffic for the interrupted window.
- **Event time.** It is per partition. Idle partitions advance on wall time only once they
  are at the end of the topic.
- **New group.** A new consumer group starts at the earliest retained detection
  (`auto.offset.reset=earliest`, up to 24 h of data). Set
  `CONSENSUS__KAFKA__CONSUMER__AUTO_OFFSET_RESET=latest` to skip an existing backlog.

## Scaling and TBMQ

> MQTT mode only, and **not deployed on the cluster** — the platform runs Kafka mode (see
> [Scaling with KEDA](#scaling-with-keda)). Kept because the mode is still supported and tested.

A tunnel's three sensors must reach the same process. For that reason tunnels are
partitioned, not messages:

- A tunnel belongs to partition `crc32(tunnel_id) % (replicas × workers)`.
- MQTT shared subscriptions would scatter one tunnel's sensors across consumers, so they
  are not used.
- With one partition, the service uses a single wildcard subscription.
- With more partitions, MQTT wildcards cannot hash. Each partition subscribes to its
  tunnels one by one: `tunnels/T000123/sensors/+/detections`, batched per SUBSCRIBE.
  This needs `partitioning.tunnels`, `index_offset` and `tunnel_id_format` to match the
  simulator.

Each worker process is one **TBMQ APPLICATION** client:

- It has a persistent session and its own Kafka-backed delivery path.
- Its client id is `consensus<partitions>p<partition>`, which is alphanumeric as TBMQ
  requires.
- Changing the partition count yields new client ids, so old sessions with stale
  subscriptions just expire.
- The install step creates the credentials with subscribe rule `tunnels/.*` and publish
  rules limited to the output topics.

At about 100k msg/s, plan roughly one worker per 60–80k msg/s, because the MQTT receive
path costs extra on top of the engine. Either:

- raise `partitioning.workers` (processes per pod), or
- raise `partitioning.replicas` (pods).

On the cluster, the service connects to `tbmq.<namespace>.svc:1883`, the in-cluster
ClusterIP. It never goes through the L4 LoadBalancer IP or Envoy.

## Limits

Kafka mode has its own list under [Kafka mode](#limits-1).

- Speeds are assumed constant between sensors, which is true for the simulator. With real
  acceleration, raise `association.midpoint_tolerance_ms` and `time_tolerance_ratio`.
- A lane change between sensors splits a vehicle, because association is per direction and
  lane.
- Output uses QoS 0 by default. Detections from QoS 0 publishers are not buffered by TBMQ
  while the service restarts.
- MQTT mode: geometry is learned per pod volume. After re-partitioning, tunnels that moved
  to another pod recalibrate once (Kafka mode shares geometry through a topic).
- Dense single-lane traffic combined with a degraded sensor can make the learned speed noise
  run away (wide gates, more mixed events). In the simulator this happens with
  `degraded_ratio: 0.3` in the busiest one-lane tunnels, with or without Kafka.

## Layout

```
config/consensus.yaml          central configuration (every key: CONSENSUS__SECTION__KEY env override)
src/tunnel_consensus/model.py  detection parsing, per-sensor dedup and statistics
src/tunnel_consensus/geometry.py  sensor spacing calibration (vote histogram), persistence
src/tunnel_consensus/engine.py    association, expiry, fusion, traffic reports
src/tunnel_consensus/sharding.py  tunnel → partition, subscriptions, client ids
src/tunnel_consensus/io.py        MQTT 5.0 session (subscribe + publish), debug outputs
src/tunnel_consensus/worker.py    MQTT input: one process per partition
src/tunnel_consensus/kafka_service.py  Kafka input: group member, engine per partition, commits, rebalances
src/tunnel_consensus/kafka_io.py  Kafka client configs, keyed outputs, geometry topic
src/tunnel_consensus/tracing.py   OpenTelemetry (only with OTEL_EXPORTER_OTLP_ENDPOINT)
src/tunnel_consensus/status.py    /healthz /readyz /metrics server, counter names
src/tunnel_consensus/__main__.py  entry point; MQTT supervisor and stats
tests/                            unit tests; end-to-end quality against the simulator model
tests/integration/                Kafka mode against Kafka 4.3.1 (SASL_SSL/SCRAM) in Docker
```
