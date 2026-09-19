# Tunnel Sensor Simulator

IoT simulator for tunnel traffic. Vehicles drive through tunnels; each tunnel has three
sensor devices (**start**, **middle**, **end**) that detect every vehicle, read its plate and
classify it into one of ten Turkish vehicle types. Detections are published over **MQTT 5.0**,
and every device also reports its own health. Sensors sometimes disagree, miss vehicles, or
send duplicates. Consensus is left to the downstream infrastructure.

Two modes:

- **profiles** (default): the curated tunnels of [`config/tunnels.yaml`](config/tunnels.yaml) —
  real names, cities, speed limits, traffic mixes and access rules (which vehicle types may not
  use the tunnel at which hours).
- **generator**: tunnels synthesized from the seed, for load tests
  (`make sim-up MODE=generator LOAD=50000`).

No broker or other infrastructure is included; point the simulator at any MQTT 5.0 broker.

## Quick start

```bash
docker build -t tunnel-sim .

# Broker running on the host
docker run --rm --network host -e SIM__MQTT__HOST=localhost tunnel-sim

# Broker on a docker network, with a custom config file
docker run --rm --network my-net \
  -v $PWD/my-config.yaml:/app/config/simulator.yaml:ro \
  -e SIM__MQTT__HOST=mosquitto tunnel-sim

# Print the effective config (file + env overrides) and exit
docker run --rm -e SIM__TOPOLOGY__TUNNELS=100 tunnel-sim --print-config
```

Without Docker:

```bash
pip install -e '.[test]'
tunnel-sim --config config/simulator.yaml
pytest
```

Stops cleanly on `SIGTERM`/`SIGINT` (`docker stop`) and exits 0. Exits 1 if a worker fails
(e.g. the broker is unreachable within `mqtt.connect_timeout_s`), 2 on invalid config.

## Configuration

All settings live in [`config/simulator.yaml`](config/simulator.yaml), which documents every
key. Any key can be overridden with an env var, using `__` for nesting:

```bash
SIM__MQTT__HOST=broker.local
SIM__TOPOLOGY__TUNNELS=500
SIM__SENSORS__MISCLASSIFICATION_RATE=0.1
SIM__SENSORS__OVERRIDES__MIDDLE__MISS_RATE=0.2   # only the middle sensors
SIM__TOPOLOGY__LENGTH_M="[1000, 2000]"
```

Values are parsed as YAML scalars or lists. Values containing `{` or `}` always stay strings,
so templates such as `SIM__MQTT__TLS__CERTFILE=/certs/{tunnel_id}.pem` work unquoted.

The config file is picked from `--config`, else `$SIM_CONFIG`, else `config/simulator.yaml`.
Unknown keys are rejected, so typos fail fast.

The main settings:

| Area | Keys |
|---|---|
| Tunnels | `topology.mode` (`profiles` / `generator`), `topology.profiles_path`, `topology.tunnels` (generator only), `simulation.workers` |
| Rate | `traffic.msgs_per_device_s`, `traffic.burst.{from,to,msgs_per_device_s}`, `traffic.target_total_msgs_s` |
| Timing | `simulation.time_scale`, `simulation.tick_ms`, `simulation.warm_start`, `simulation.duration_s` |
| Traffic | `traffic.start_to_end_ratio`, `traffic.violation_factor`, `traffic.speeding_per_day`; the type mix, speed limit and access rules come from the tunnel's profile |
| Sensor errors | `sensors.misclassification_rate`, `unknown_rate`, `miss_rate`, `duplicate_rate`, `timestamp_jitter_ms`, `speed_noise_ratio`, `length_noise_std_m`, `degraded_ratio`, `overrides.<position>` |
| MQTT | `mqtt.host/port/qos/transport/tls/username/password`, `topic_template`, `message_expiry_s`, `user_properties` |
| Connections | `mqtt.connection_mode` (`shared` / `per_tunnel`), `connections_per_worker`, `connect_rate_per_s`, `connect_timeout_s`, `reconnect_delay_s` |
| Evaluation | `payload.include_vehicle_id`, `payload.include_true_class` (ground truth, off by default) |
| Service | `service.http_port` (`/metrics`, `/healthz`, `/time`), `service.health_interval_s`, `service.device_metrics_max` |
| Debug | `output.sink`: `mqtt`, `stdout`, or `discard` (no network, for generation benchmarks) |

The expected message rate is logged at startup:
`tunnels × 3 devices × msgs_per_device_s × (1 − miss_rate) × (1 + duplicate_rate)`.
The default profile deployment (5 tunnels, 2 msg/s per device) is **30 msg/s**, rising to 45 in
the evening burst. A load test states a total instead: `LOAD=50000` sets
`traffic.target_total_msgs_s`, and the per-device rate follows from the tunnel count.

## Simulation model

- **Geometry.** Each tunnel is `length_m` long, with sensors at x = 0 (start), L/2 (middle)
  and L (end). Length, lane count and traffic-rate multiplier are fixed per tunnel,
  derived from `seed`.
- **Traffic.** Poisson arrivals per tunnel at `traffic.msgs_per_device_s` (one vehicle produces
  one detection per device, so that is both the vehicle rate and the per-device message rate),
  rising to the burst rate between 17:00 and 20:00 local time. Every vehicle gets a type, a
  plate, a length, a constant speed, a lane and a direction.
- **Vehicle types.** Ten Turkish types (`MOTOSIKLET`, `TRAKTOR`, `OTOMOBIL`, `HAFIF_TICARI`,
  `MINIBUS`, `OZEL_AMACLI_TASIT`, `KAMYON`, `OTOBUS`, `CEKICI_YARI_ROMORK`, and `BILINMEYEN`
  which only a sensor reports). Which ones appear is the tunnel's `type_mix`; their lengths and
  free speeds come from the type and the tunnel's speed limit
  ([`vehicles.py`](src/tunnel_sim/vehicles.py)).
- **Plates.** Every vehicle gets a Turkish plate (`34 ABC 123`), unique by construction: mostly
  the tunnel's own province, with a tail of neighbouring and big-city codes.
- **Access rules.** A tunnel's profile can ban vehicle types during part of the day. Banned
  types are not impossible: their share is multiplied by `traffic.violation_factor` (2 %), so a
  few still pass — those are what the alerts report.
- **Speeding.** Normal traffic cruises just under the posted limit; `traffic.speeding_per_day`
  vehicles per tunnel per day (2–3, with spread) exceed it.
- **Consistency.** The front of the vehicle reaches a sensor at
  `t_entry + distance/speed`. An `end_to_start` vehicle hits end → middle → start. Reported
  `speed_kmh`, `length_m` and `occupancy_ms` agree with each other
  (`occupancy ≈ length / speed`) and with the time between sensors.
- **Classification.** A sensor reports the vehicle's type. `misclassification_rate` makes it
  report a neighbouring size class instead (a `MINIBUS` for a `HAFIF_TICARI`, never a
  `MOTOSIKLET` for a `CEKICI_YARI_ROMORK`), and `unknown_rate` makes the reading too ambiguous
  to classify (`BILINMEYEN`). Reported `confidence` follows how far the length estimate sits
  from the nearest size boundary, relative to the sensor's own noise, so borderline vehicles
  are reported with low confidence.
- **Device identity.** Each device has a stable MAC address (`02:...`, derived from the seed and
  the sensor id) and a serial number, reported in every message.
- **Device health.** Every `service.health_interval_s` each device publishes what it did in the
  window (detections, misses, duplicates, `ok` / `degraded` / `silent`) on
  `tunnels/{tunnel_id}/sensors/{position}/health`, retained, and the same counters are exposed
  per device on `/metrics`.
- **Faults.** Missed detections, duplicates (same `message_id`, re-sent after
  `duplicate_delay_ms`), and timestamp and speed noise. A `degraded_ratio` share of sensors
  get all their error rates multiplied by `degraded_multiplier`, so some sensors are
  consistently worse than others.
- **Real time.** Events are published when they happen on the wall clock (scaled by
  `time_scale`). `ts` is always the exact simulated detection time, even if publishing is
  late by up to `tick_ms` or because of load (see `max_lag` in the logs). With
  `warm_start`, tunnels already contain vehicles at startup.
- **Simulated time of day.** The clock starts on the host clock and is read in Europe/Istanbul,
  which is what drives the evening burst, the access rules and the "daily" dashboards. It can be
  moved at runtime and put back:

  ```bash
  curl -XPOST host:9109/time -d '{"set": "2026-06-15T18:00:00"}'   # jump to 18:00 local
  curl -XPOST host:9109/time -d '{"offset_s": 3600}'               # shift by an hour
  curl -XPOST host:9109/time/resync                                # back to host time
  curl host:9109/time                                              # what the simulation thinks it is
  ```

  The offset is in memory only: restarting the container resyncs it with the host clock.

## MQTT interface

**Detections.** Topic `tunnels/{tunnel_id}/sensors/{position}/detections`, set by
`mqtt.topic_template`, QoS `mqtt.qos` (default 0).

```json
{
  "schema": 2,
  "message_id": "TR-BOLU-middle:9f3a1c:1234",
  "sensor_id": "TR-BOLU-middle",
  "device_id": "02:4b:1f:aa:31:07",
  "device_serial": "TSN-QUT73ELQA5",
  "tunnel_id": "TR-BOLU",
  "position": "middle",
  "seq": 1234,
  "ts": 1789377527984,
  "direction": "end_to_start",
  "lane": 1,
  "plate": "14 ABC 123",
  "speed_kmh": 66.2,
  "length_m": 16.02,
  "occupancy_ms": 871,
  "classification": "KAMYON",
  "confidence": 0.99
}
```

**Health.** Topic `tunnels/{tunnel_id}/sensors/{position}/health`, retained, QoS 1:

```json
{
  "schema": 2,
  "sensor_id": "TR-BOLU-middle",
  "device_id": "02:4b:1f:aa:31:07",
  "device_serial": "TSN-QUT73ELQA5",
  "tunnel_id": "TR-BOLU",
  "position": "middle",
  "ts": 1789377527984,
  "status": "ok",
  "degraded": false,
  "window_s": 60.0,
  "uptime_s": 3600.0,
  "detections": 118,
  "duplicates": 1,
  "missed": 2
}
```

| Field | Meaning |
|---|---|
| `message_id` | `sensor_id:boot_id:seq`. Unique per detection; a duplicate repeats it. `boot_id` changes on every simulator start. |
| `seq` | Per-sensor counter, increases by one per detection. Duplicates reuse it. Missed vehicles consume no `seq`. |
| `ts` | Detection time, epoch milliseconds (includes jitter). |
| `direction` | `start_to_end` or `end_to_start`. |
| `device_id`, `device_serial` | The physical device that produced the detection (MAC + serial); stable across restarts. |
| `plate` | Turkish plate of the vehicle, as the device read it. |
| `classification` | One of the ten types; `BILINMEYEN` when the device could not tell. |
| `vehicle_id`, `true_class` | Present only when enabled under `payload.*`. |

MQTT 5.0 features used:

- PUBLISH properties: `Payload Format Indicator = 1`, `Content Type = application/json`,
  `Message Expiry Interval`, and User Properties
  `tunnel_id`, `sensor_id`, `device_id`, `position`, `schema`, so consumers can route without
  parsing JSON.
- CONNECT with `Clean Start` and `Session Expiry Interval`.
- Last Will: retained `offline` on `simulator/{client_id}/status`. `online` is published on
  connect and `offline` on clean shutdown.

### Connection modes

`mqtt.connection_mode` decides how sensors map onto MQTT connections.

- **`shared`** (default). Each worker opens `mqtt.connections_per_worker` connections, and
  its tunnels share them. Client IDs are `{client_id_prefix}-w{worker}-c{connection}`.
- **`per_tunnel`**. Each tunnel gets exactly one connection, which carries the detections
  of all three of its sensors. Client ID is `{client_id_prefix}-{tunnel_id}`, e.g.
  `tunnel-sim-T000123`, and the status topic is `simulator/tunnel-sim-T000123/status`.
  `connections_per_worker` is ignored. Use this when the broker authenticates every tunnel
  as its own device.

If you run several simulator instances, give each one a separate `topology.index_offset`
range. In `shared` mode, also give each its own `mqtt.client_id_prefix`.

**Per-tunnel mutual TLS.** `mqtt.tls.certfile` and `keyfile` may contain `{tunnel_id}` in
`per_tunnel` mode. The certificate file holds the leaf certificate followed by its issuing
CA chain, and the whole chain is sent during the handshake. A `{tunnel_id}` placeholder in
`shared` mode is a config error (exit 2).

```bash
SIM__MQTT__CONNECTION_MODE=per_tunnel
SIM__MQTT__PORT=8883
SIM__MQTT__TLS__ENABLED=true
SIM__MQTT__TLS__CA_CERTS=/certs/ca.pem          # verifies the broker
SIM__MQTT__TLS__CERTFILE=/certs/{tunnel_id}.pem # CN = tunnel id, leaf + CA chain
SIM__MQTT__TLS__KEYFILE=/certs/{tunnel_id}.key
```

A broker ACL can then restrict each certificate to `tunnels/<CN>/sensors/+/(detections|health)`
and its own status topic.

In `per_tunnel` mode a worker does not start one thread per connection. It uses one network
thread that multiplexes all its connections through a selector, and 4 threads for the
blocking part of connecting (TCP connect and TLS handshake). Connection attempts are paced
at `mqtt.connect_rate_per_s` across all workers. Reconnects use exponential backoff within
`reconnect_delay_s` with random jitter, so a broker restart does not cause a reconnect storm.
At startup a worker waits as long as connections keep succeeding. It fails (exit 1) only if
none of its connections succeeded within `connect_timeout_s`. Connections that are still
down keep retrying in the background, and their detections are counted as dropped.

## Metrics

With `service.http_port` set, the supervisor serves `/metrics` (Prometheus text), `/healthz` and the
time control endpoints (`GET`/`POST /time`, `POST /time/resync`):
`tunnel_sim_published_total`, `_dropped_total`, `_missed_total`, `_duplicates_total`, `_vehicles_total`,
`tunnel_sim_clients_connected` / `_clients`, `tunnel_sim_workers*`, `tunnel_sim_max_lag_seconds`,
`tunnel_sim_pending_messages`, `tunnel_sim_scheduled_vehicles`. The same numbers as the stats log line.

Per device (labels `device_id`, `tunnel_id`, `position`): `tunnel_sim_device_published_total`,
`_missed_total`, `_duplicates_total`, `_dropped_total`, `tunnel_sim_device_degraded`. These are only
exposed while there are at most `service.device_metrics_max` devices (300 by default), so a load test
with 25,000 tunnels does not turn into 75,000 series per metric — at that scale device-level history
is in ClickHouse (`iot.detections`, `iot.sensor_health`).

`make sim-up` switches it on (`SIM_METRICS_PORT`, default 9109) and binds it to the cluster's node
address (`K3S_NODE_IP`), so the platform scrapes the sender as a static target and Grafana can compare
what was sent with what arrived. `service.http_bind` defaults to `0.0.0.0` for standalone runs.

## Performance

A supervisor process starts `simulation.workers` worker processes (default: available CPUs,
at most 16). Tunnels are split across workers. Each worker runs a heap-based discrete-event
scheduler and owns its own MQTT connection(s).

Two optimisations matter most. MQTT5 properties are packed once per sensor instead of once
per publish, and publishes go out in batches every `tick_ms`.

Measured on a 24-thread machine against a local Mosquitto 2 broker, with default config:

- ~99.5–100k msg/s sustained, 0 dropped
- Scheduler lag mostly under 20 ms
- ~5.4 cores
- ~1.7 GB RSS, from ~2.5M vehicles in flight across 25k tunnels

A single worker core can generate about 150k msg/s before MQTT overhead.

**Backpressure.** If the broker falls behind or disconnects, each connection buffers up to
`mqtt.max_pending_messages`. Beyond that, messages are dropped and counted in `dropped=`, so
memory stays bounded. paho reconnects automatically with backoff.

In `per_tunnel` mode the limit applies per worker, summed over all of its connections.
Detections for a tunnel whose connection is down are dropped right away. The stats line
shows `clients=connected/total` across all workers.

Measured for `per_tunnel` against a local Mosquitto 2, with 2,000 tunnels on a single worker
(default traffic, about 7,900 msg/s):

- All 2,000 connections up in 4 s
- 0 dropped
- Scheduler lag under 10 ms
- 8 OS threads in the worker
- About half a core
- RSS 177 MB without TLS, 279 MB with mutual TLS (about 50 KB per TLS connection)

Use QoS 1/2 for reliability testing only: throughput is far lower.

**Reproducibility.** Tunnels depend only on `seed`. The traffic sequence depends on `seed`
and the worker count.

## Layout

```
config/simulator.yaml      central configuration
src/tunnel_sim/config.py   config schema, validation, env overrides
src/tunnel_sim/profiles.py tunnel profiles: geometry, speed limit, type mix, access rules
src/tunnel_sim/vehicles.py the ten vehicle types and their physical parameters
src/tunnel_sim/plates.py   Turkish plate numbers, unique by construction
src/tunnel_sim/devices.py  per-device MAC address and serial number
src/tunnel_sim/clock.py    simulated time of day (Europe/Istanbul, host-synced, overridable)
src/tunnel_sim/model.py    tunnels, vehicles, sensor physics and error model
src/tunnel_sim/payload.py  JSON encoding
src/tunnel_sim/sinks.py    MQTT 5.0 (shared connections) / stdout / discard outputs
src/tunnel_sim/per_tunnel.py MQTT 5.0 with one connection per tunnel, multiplexed I/O
src/tunnel_sim/worker.py   per-process real-time event scheduler
src/tunnel_sim/__main__.py supervisor, stats, shutdown
tests/                     physics, fault model, config tests
```
