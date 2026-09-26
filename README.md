# IoT infrastructure

A single-node k3s prototype of the architecture in `temp/aus-iot-report.pdf`: tunnel sensors publish over
mutual-TLS MQTT to TBMQ, and a Kafka-based pipeline turns their detections into vehicle events stored in
ClickHouse, with KEDA autoscaling and a VictoriaMetrics/Logs/Traces observability stack. The tunnel sensor
simulator produces the traffic. Run `make help` for all targets.

```
simulator ─MQTTS 8883, client cert per tunnel─▶ TBMQ (Cilium L4) ─▶ TBMQ internal Kafka (Strimzi, mTLS)
  ─▶ integration executors ─ Kafka integrations app-kafka-ingest-<position>-<shard> (KEDA) ─▶ app Kafka iot.mqtt.ingest
  ─▶ telemetry-processor (KEDA)  validates topic vs certificate CN, re-keys by tunnel ─▶ iot.detections
                                 device health reports ────────────────────────────────▶ iot.sensor-health
  ─▶ consensus (KEDA)            fuses start/middle/end detections ─▶ iot.vehicles, iot.traffic
  ─▶ ClickHouse Kafka engine     tables, deduplicated vehicles, 1m/1h rollups ─TTL─▶ SeaweedFS S3 cold tier
                                 speed-limit and restricted-vehicle alerts: views over the vehicles
                                 joined to the tunnel profiles and rules (Grafana "Tunnel operations")
```

## Layers

| Layer | Components | Namespace |
|---|---|---|
| Platform | k3s (node IP on a dummy interface), Cilium (CNI, kube-proxy replacement, L4 LB, Hubble), Envoy Gateway, cert-manager | kube-system, envoy-gateway-system, cert-manager |
| Operators | Strimzi, CloudNativePG, Stakater Reloader, KEDA | strimzi-system, cnpg-system, reloader, keda |
| MQTT broker | TBMQ 2.4.0 + integration executors; internal Kafka (Strimzi), Postgres (CNPG), Valkey | thingsboard-mqtt-broker |
| Event backbone | App Kafka `app-kafka` (Strimzi): TLS + SCRAM-SHA-512, ACLs per client | iot-pipeline |
| Stream processing | telemetry-processor (stateless), consensus (stateful per tunnel) | iot-pipeline |
| Storage (`STORAGE_ENABLED`) | ClickHouse (hot + rollups), SeaweedFS (S3 cold tier) | iot-storage |
| Observability (`OBSERVABILITY_ENABLED`) | VictoriaMetrics + vmalert + Alertmanager + Grafana, VictoriaLogs + Vector, VictoriaTraces + OpenTelemetry Collector | monitoring |

Replica counts, heaps, memory limits, retention and partition counts are variables in `config.env`. The
defaults are lean for a laptop, and replication factors follow the replica counts.

## Install

```bash
make install LEGACY_DEPS_DELETE=true   # first switch from the old upstream Postgres/Kafka/Valkey deletes their data
make sim-up                            # simulator: device certificates + one mTLS connection per tunnel
make verify
```

`make install` runs the steps in order:

1. `prereqs`, `k3s` (sudo), `cilium`, `lb-ipam`, `cert-manager`, `envoy-gateway`
2. `operators`, `tbmq-deps`, `app-kafka`, `tbmq`
3. `processing` (sudo: image import), `storage`, `observability`

Every step is idempotent and can run alone (`make install-<step>`), as can every check
(`make verify-<step>`). `make down` / `make up` stop and start the workloads while keeping data: KEDA is
paused, Kafka reconciliation is paused and its brokers removed, and Postgres is hibernated.

## Stream processing and scaling (KEDA)

- **Integration executors.** TBMQ gives each integration a single-partition topic (for message order) and
  assigns each integration to one executor. `TBMQ_IE_SHARDS` tunnel groups x 3 sensor positions
  integrations let that many executors run: a shard subscribes to its tunnels by name (`+` matches a
  whole level, so the tunnel level cannot be split by wildcard). KEDA scales the executors on the lag
  of those topics in the internal Kafka.
- **telemetry-processor.** Consumes the raw integration envelopes and rejects messages whose topic tunnel
  doesn't match the client certificate CN (`iot.detections.rejected`). It re-keys detections by tunnel
  id and commits offsets only after its output is acknowledged. KEDA scales it on Kafka lag, up to the
  `iot.mqtt.ingest` partitions.
- **consensus.** A pod owns the tunnels of its Kafka partitions. It commits only past detections that are
  already fused into emitted vehicles, keeps learned tunnel geometry in the compacted
  `iot.consensus.geometry` topic, and re-reads without re-emitting after a rebalance. Duplicates are
  possible; losses are not. Because its committed offset trails on purpose, KEDA scales it on
  `consensus_kafka_lag_records` from VictoriaMetrics rather than on Kafka lag; without the observability
  layer it runs a fixed replica count.
- **No node autoscaling.** Karpenter is out of scope on a single node.

`make watch-topic TOPIC=iot.vehicles` prints records live (read-only KafkaUser `pipeline-viewer`).

### Capacity

Measured on the development node (i7-13700HX, 16C/24T, 32 GB) at ~3,950 msg/s, 1000 tunnels, QoS 0.
Cost per message is CPU divided by the pipeline message rate, in millicores per msg/s:

| Stage | mc per msg/s | share |
|---|---|---|
| TBMQ (2 pods) | 0.251 | 31% |
| TBMQ internal Kafka | 0.205 | 26% |
| Integration executors | 0.095 | 12% |
| app Kafka | 0.095 | 12% |
| ClickHouse | 0.062 | 8% |
| consensus | 0.037 | 5% |
| telemetry-processor | 0.027 | 3% |
| SeaweedFS, Postgres, Valkey | 0.023 | 3% |

Regressed over 6 h and 0–5,825 msg/s: **CPU = 0.35 cores fixed + 0.732 millicore per msg/s**, i.e.
**1,366 msg/s per marginal core**. The broker and its internal Kafka are 57% of it, because every
message is persisted to Kafka before an integration executor sees it; the two Python services are 8%,
the cheapest stages, since librdkafka does their I/O in C and they consume in batches.

With ~18 cores left for the pipeline after the OS, Cilium and the observability stack, this node
sustains roughly **20–25k msg/s** end to end. Scaling out from there:

| Target | Cores | Nodes of this class |
|---|---|---|
| 20k msg/s | 15 | 1 (this one) |
| 100k msg/s | 74 | ~4 |
| 300k msg/s | 220 | ~10 |

> **Querying CPU here:** cAdvisor exports every container twice, once with an `image` label and once
> without. `sum(rate(container_cpu_usage_seconds_total{container!=""}[5m]))` therefore double-counts;
> always add `image!=""`. The `Pipeline economics` dashboard does, and `kubectl top` (metrics-server)
> is an independent cross-check.

What the defaults size for a 300k target anyway, because these cannot be changed cheaply later:

- **Partitions** (`APP_KAFKA_INGEST_PARTITIONS`, `APP_KAFKA_TUNNEL_PARTITIONS`, 48 each) cap how far
  KEDA can scale each consumer group. Kafka can only grow a topic's partitions, and growing a keyed
  topic moves tunnels between consensus pods, so pick the target count up front.
- **Integration shards** (`TBMQ_IE_SHARDS`) decide how many integration executors can run at all: TBMQ
  gives every integration a single-partition topic and one executor, so integrations = the ceiling.
- **Retention** is bounded by bytes, not only time (`APP_KAFKA_*_RETENTION_BYTES`): at 300k msg/s each
  hop writes ~75 MB/s, which no time-based retention can hold on one disk.
- **Consumer batching** (`TBMQ_MSG_CONSUMER_CONFIG`, `TBMQ_IE_MSG_CONSUMER_CONFIG`) is where the
  broker path's overhead lives. TBMQ commits synchronously once per processed pack, and with no batch
  floor a pack averaged 2.87 messages, so its internal Kafka served 0.41 RPCs and wrote 0.83 offset
  records per message where the app Kafka needed 0.14 RPCs per record. A `fetch.min.bytes` floor and a
  `fetch.max.wait.ms` window make each poll return a real batch, at the cost of up to that window in
  added latency per hop — negligible against dashboards that show ~10 s data age.

`make sim-up` runs the curated tunnels of `apps/simulator/config/tunnels.yaml` (real Turkish tunnels with
their own speed limits, traffic mixes and access rules) at 2 messages per second per device, 3 during the
17:00-20:00 burst. Load testing is a separate mode: `make sim-up MODE=generator LOAD=50000` synthesizes
tunnels and sizes the per-device rate to that total, which loads the pipeline without paying for another
TLS connection and device certificate per tunnel. The simulator runs on the host and costs about one core
per 5k msg/s, so past ~10k msg/s generate the load from another machine (`SIM__MQTT__HOST` points at
`MQTT_LB_IP`) instead of stealing cores from the broker.

## Storage

ClickHouse consumes the app Kafka topics directly (Kafka engine, KafkaUser `clickhouse`) into database `iot`:

- **Raw data:** `detections`, with MQTT/processing/ingest latency columns, and `detections_rejected`.
- **Vehicles:** `vehicles` (ReplacingMergeTree on `event_id`; exact counts via `vehicles_dedup`).
- **Rollups:** `vehicles_1m` / `vehicles_1h`, fed only with first-seen event ids.
- **Reports:** `traffic` and `sensor_health`.

TTLs move aged parts to the SeaweedFS S3 disk and delete them later (`CLICKHOUSE_*_AFTER`). Grafana reads
with the read-only user `grafana`. Use `make clickhouse-client` for SQL.

## Observability

- **Metrics:** vmagent scrapes TBMQ and its integration executors, both Kafka clusters (JMX exporter and
  Kafka Exporter), CNPG, Valkey (redis_exporter sidecar), ClickHouse, SeaweedFS, KEDA, Cilium/Hubble,
  Envoy Gateway, cert-manager, the pipeline services and the OTel Collector.
- **Logs and traces:** Vector ships every pod log to VictoriaLogs. The Python services send sampled
  traces (W3C `traceparent` in Kafka headers) through the OTel Collector to VictoriaTraces.
- **Alerts:** VMRule `iot-platform` covers the report's critical alerts: MQTT connections and reconnect
  storms, publish rate, delivery latency, JVM heap, Kafka lag, under-replicated partitions and ISR shrink,
  CNPG failover and replication, Valkey memory, ClickHouse inserts/parts/disk, plus pipeline and platform
  health. Alertmanager has no external receivers.
- **Dashboards (Grafana folder IoT):** pipeline overview (with a live row), Kafka, TBMQ, tunnel
  traffic (ClickHouse SQL), and **Pipeline economics** — per component: CPU/memory consumed,
  throughput and loss at every hop, latency per hop, cost in millicores per msg/s, and the Kafka
  RPC-per-record contrast between the TBMQ-internal and application clusters. Its TLS panels are
  connection and byte proxies: nothing in the stack attributes CPU to TLS. Regenerate them with
  `deploy/observability/dashboards/generate.py`.
- **Simulator.** `make sim-up` exposes `tunnel_sim_*` metrics on `K3S_NODE_IP:SIM_METRICS_PORT`, scraped as
  a static target, so the panels compare what was sent with what arrived. Its target is down whenever the
  simulator is not running, which the alerts ignore on purpose.

### How fresh the panels are

| Panel source | Delay |
|---|---|
| Metrics (rates, lag, connections, replicas) | 10 s scrape + 5 s query offset + refresh → **~10–25 s** |
| ClickHouse (vehicles, detections, rejections, ingest latency) | ~5 s ingest + query, refresh 10 s |
| Logs (VictoriaLogs) | seconds |
| Traces (VictoriaTraces) | seconds, 1% sampled, pipeline services only |

Dashboards refresh every 10 s (30 s on the reporting view) and the refresh picker allows 5 s. The live row
of the pipeline overview shows data-path freshness, ingest latency, a sent-versus-received panel across all
seven stages, simulator detail, and tables of the last vehicles and rejections. Faster scraping costs about
96 MiB more memory in `monitoring`; alerts evaluate every 15 s for the MQTT, Kafka and pipeline groups.

`make endpoints` prints the port-forward commands for Grafana, vmui, vmalert, VictoriaLogs and
VictoriaTraces. The chart sync job and the Grafana plugins download from the internet at install and start.

### The TBMQ UI

`http://EDGE_LB_IP/` (or https) shows the broker side live: Sessions (every connected device with its
certificate-derived client id), Home and Monitoring (session counts, message rates, health), Integrations
with their state and counters, the internal Kafka's topics and consumer groups, retained messages,
subscriptions, and the credentials, providers and rejected connections under Authentication.

Two pages do not apply here. The **WebSocket client** cannot connect: TBMQ's WebSocket listener is off
and unexposed, because Envoy terminates TLS so a device certificate could never reach TBMQ, and MQTT
Basic is disabled. Use `make watch-topic TOPIC=iot.vehicles` for live payloads. Everything downstream of
the broker (app Kafka, processing, ClickHouse) is in Grafana, not in this UI.

## Security

- **Devices.** Mutual TLS on `mqtts://MQTT_LB_IP:8883`, the only MQTT port on the LoadBalancer. Client
  certificates are signed by the cert-manager CA `iot-device-ca` (`make device-certs`, CN = tunnel id). TBMQ
  maps the CN to ACLs (`deploy/tbmq/device-credentials.json.tpl`): a tunnel may only publish its own
  detection and status topics.
- **Kafka.** The internal Kafka requires TLS with client certificates. The app Kafka uses TLS + SCRAM with
  least-privilege ACLs per client. Strimzi NetworkPolicies, enforced by Cilium, admit only the listed
  client pods.
- **Certificates only.** The MQTT Basic provider is disabled, because TBMQ's installer creates built-in
  WebSocket credentials with no password and allow-all rules, and it refuses to delete them: with the
  provider enabled, any pod (in-cluster 1883) or any holder of a device-CA certificate could publish and
  subscribe to everything. `make install-tbmq` disables the provider and deletes any other MQTT Basic
  credentials; `make verify-tbmq` checks it. X.509 stays enabled — with no provider enabled at all, TBMQ
  accepts every client.
- **TBMQ admin.** A generated password (`.state/tbmq-admin-password`) and a generated JWT signing key
  (TBMQ's default key is public). `/actuator` (unauthenticated metrics) is blocked at the Envoy Gateway.
- **Host exposure.** LB IPs are host-local and no NodePorts are allocated. k3s runs on a stable dummy
  interface address (`K3S_NODE_IP`), so Wi-Fi/DHCP changes don't break it.
- **Renewals.** Reloader restarts TBMQ, the integration executors, ClickHouse and the pipeline services
  when cert-manager or Strimzi renews certificates. Copies held outside Kubernetes secrets are refreshed
  by re-running the owning step: the app Kafka CA in the TBMQ integrations (`make install-tbmq`) and in
  ClickHouse (`make install-storage`).

## Layout

```
Makefile               entry point; every target wraps a script in scripts/
config.env             configuration defaults (override in config.local.env or make VAR=value)
apps/
  simulator/           tunnel sensor simulator (Python), MQTT 5 per-tunnel mTLS connections, /metrics
  telemetry-processor/ envelope validation and re-keying (Python, Kafka)
  consensus/           detection fusion into vehicle events (Python, Kafka or MQTT)
deploy/                manifests, Helm values and templates (*.tpl rendered by scripts)
  k3s/ cilium/ lb-ipam/ cert-manager/ envoy-gateway/   platform
  operators/           Strimzi, CloudNativePG, Reloader, KEDA Helm values
  tbmq-deps/           internal Kafka, Postgres, Valkey; schema install pod
  tbmq/                broker patches, data-clients component, X.509 credentials, Kafka integrations
  app-kafka/           app Kafka cluster, topics, SCRAM users + ACLs
  processing/          telemetry-processor, consensus, KEDA ScaledObjects
  storage/             ClickHouse + SeaweedFS manifests, ClickHouse config and schema
  observability/       Helm values, scrape targets, alert rules, dashboards, Kafka JMX rules
scripts/
  lib.sh               shared helpers and config loading, sourced by every script
  install/NN-*.sh      install steps, run in order by make install
  verify.sh up.sh down.sh sim.sh endpoints.sh kafka-watch.sh device-certs.sh
  tbmq_credentials.py  TBMQ REST: admin password, credentials, X.509 provider, Kafka integrations
  probe.py             MQTT 5 / WebSocket probes used by verify.sh
vendor/
  tbmq/                upstream TBMQ checkout; k8s/minikube manifests used unmodified
.generated/  .state/   written by make install (gitignored)
```
