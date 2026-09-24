#!/usr/bin/env python3
"""Generate the IoT Grafana dashboards.

    python3 deploy/observability/dashboards/generate.py deploy/observability/dashboards

Edit this file, never the generated JSON. The dashboards are applied as ConfigMaps by
scripts/install/90-observability.sh (kustomization.yaml, Grafana sidecar label grafana_dashboard=1).
Datasource UIDs must match deploy/observability/victoria-metrics-k8s-stack.yaml: VictoriaMetrics,
VictoriaLogs, VictoriaTraces, ClickHouse, Alertmanager.

Live watching: the metric path is scraped every 10s for the pipeline jobs, VMSingle makes samples
queryable after 5s (-search.latencyOffset), and these dashboards refresh every 10s over the last 30
minutes. ClickHouse panels query the database directly and are only bounded by the insert path.
"""
import json
import sys
from pathlib import Path

OUT = Path(sys.argv[1])
VM = {"type": "prometheus", "uid": "VictoriaMetrics"}
VL = {"type": "victoriametrics-logs-datasource", "uid": "VictoriaLogs"}
CH = {"type": "grafana-clickhouse-datasource", "uid": "ClickHouse"}


class Board:
    def __init__(self, uid, title, tags, description, variables=(), refresh="10s", time_from="now-30m"):
        self.uid, self.title, self.tags, self.description = uid, title, tags, description
        self.variables, self.refresh, self.time_from = list(variables), refresh, time_from
        self.panels, self.next_id, self.y, self.x, self.row_h = [], 1, 0, 0, 0

    def _id(self):
        i = self.next_id
        self.next_id += 1
        return i

    def row(self, title):
        self.newline()
        self.panels.append({"type": "row", "title": title, "id": self._id(), "collapsed": False,
                            "gridPos": {"h": 1, "w": 24, "x": 0, "y": self.y}, "panels": []})
        self.y += 1

    def newline(self):
        if self.x:
            self.y += self.row_h
            self.x, self.row_h = 0, 0

    def add(self, panel, w, h):
        if self.x + w > 24:
            self.newline()
        panel["id"] = self._id()
        panel["gridPos"] = {"h": h, "w": w, "x": self.x, "y": self.y}
        self.panels.append(panel)
        self.x += w
        self.row_h = max(self.row_h, h)

    def json(self):
        return {
            "uid": self.uid, "title": self.title, "tags": self.tags, "description": self.description,
            "editable": False, "graphTooltip": 1, "schemaVersion": 41, "version": 1,
            "time": {"from": self.time_from, "to": "now"}, "timezone": "browser", "refresh": self.refresh,
            "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m", "1h"],
                           "time_options": ["5m", "15m", "30m", "1h", "3h", "6h", "12h", "24h", "7d", "30d"]},
            "templating": {"list": self.variables}, "annotations": {"list": []}, "links": [
                {"type": "dashboards", "tags": ["iot"], "asDropdown": True, "title": "IoT dashboards",
                 "includeVars": False, "keepTime": True}],
            "panels": self.panels,
        }


def prom(expr, legend="", instant=False):
    t = {"datasource": VM, "expr": expr, "legendFormat": legend or "__auto", "refId": ""}
    if instant:
        t.update(instant=True, range=False)
    return t


def ch(sql, fmt="timeseries"):
    """ClickHouse query. Live panels use a fixed short window + LIMIT instead of the dashboard range."""
    return {"datasource": CH, "editorType": "sql", "rawSql": sql, "format": 0 if fmt == "timeseries" else 1,
            "queryType": fmt, "pluginVersion": "4.21.3", "refId": ""}


def empty(no_value):
    """Panel text for an empty result: "No data" cannot tell a broken query from nothing to report."""
    return {"noValue": no_value} if no_value else {}


def with_refs(targets):
    for i, t in enumerate(targets):
        t["refId"] = chr(ord("A") + i)
    return targets


def ts(title, targets, unit="short", description="", stack=False, min0=True, legend_calcs=("lastNotNull", "max"),
       no_value=None, overrides=None):
    ds = targets[0]["datasource"]
    return {
        "type": "timeseries", "title": title, "description": description, "datasource": ds,
        "targets": with_refs(targets),
        "fieldConfig": {"defaults": {"unit": unit, **({"min": 0} if min0 else {}), **empty(no_value),
                                     "custom": {"lineWidth": 1, "fillOpacity": 10, "showPoints": "never",
                                                "spanNulls": True,
                                                "stacking": {"mode": "normal" if stack else "none", "group": "A"}}},
                        "overrides": overrides or []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": list(legend_calcs)},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def stat(title, target, unit="short", description="", thresholds=None, decimals=None, color_mode="value",
         no_value=None):
    steps = thresholds or [{"color": "green", "value": None}]
    defaults = {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps}, "color": {"mode": "thresholds"},
                **empty(no_value)}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "type": "stat", "title": title, "description": description, "datasource": target["datasource"],
        "targets": with_refs([target]),
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": color_mode, "graphMode": "area", "textMode": "value_and_name",
                    "justifyMode": "auto", "orientation": "auto"},
    }


def bars(title, targets, unit="short", description="", decimals=None, max_value=None, thresholds=None):
    """Horizontal bar gauge: ranks a handful of instant values (which stage costs most)."""
    defaults = {"unit": unit, "min": 0, "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": thresholds or [{"color": "green", "value": None}]}}
    if decimals is not None:
        defaults["decimals"] = decimals
    if max_value is not None:
        defaults["max"] = max_value
    return {
        "type": "bargauge", "title": title, "description": description, "datasource": targets[0]["datasource"],
        "targets": with_refs(targets), "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                    "valueMode": "color", "namePlacement": "left", "minVizWidth": 8, "minVizHeight": 16,
                    "maxVizHeight": 300, "sizing": "auto", "legend": {"showLegend": False}},
    }


def table(title, targets, description="", transformations=None, overrides=None, no_value=None):
    for t in targets:
        # A Prometheus query only comes back as a table frame with this; without it Grafana
        # returns one field per series, so label columns (and joinByField) simply are not there.
        if t["datasource"] is VM:
            t["format"] = "table"
    return {
        "type": "table", "title": title, "description": description, "datasource": targets[0]["datasource"],
        "targets": with_refs(targets), "transformations": transformations or [],
        "fieldConfig": {"defaults": {"custom": {"align": "auto"}, **empty(no_value)}, "overrides": overrides or []},
        "options": {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}},
    }


def logs(title, expr, description="", no_value=None):
    return {
        "type": "logs", "title": title, "description": description, "datasource": VL,
        "fieldConfig": {"defaults": empty(no_value), "overrides": []},
        "targets": with_refs([{"datasource": VL, "expr": expr, "queryType": "instant", "editorMode": "code",
                               "maxLines": 200, "refId": ""}]),
        "options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending", "enableLogDetails": True,
                    "dedupStrategy": "none", "prettifyLogMessage": False},
    }


def series_unit(name, unit):
    """Override one series' unit: some panels deliberately mix counts, seconds, bytes and ratios."""
    return {"matcher": {"id": "byRegexp", "options": name}, "properties": [{"id": "unit", "value": unit}]}


RED = [{"color": "green", "value": None}, {"color": "red", "value": 1}]
ALERT_STEPS = [{"color": "green", "value": None}, {"color": "orange", "value": 1}]
RATE = "$__rate_interval"

# =========================================================================================================
# IoT pipeline overview
# =========================================================================================================
b = Board("iot-pipeline-overview", "IoT pipeline overview", ["iot", "pipeline"],
          "MQTT ingress -> TBMQ -> integration executor -> app Kafka -> telemetry-processor -> consensus -> ClickHouse.")
b.row("Live")
b.add(stat("Data path freshness", ch("SELECT if(count() = 0, NULL, dateDiff('second', max(inserted_at), now())) AS seconds_behind\n"
                                     "FROM iot.detections\nWHERE inserted_at >= now() - INTERVAL 10 MINUTE", "table"),
           unit="s", decimals=0, no_value="nothing stored in the last 10 min",
           description="Age of the newest row in iot.detections: sensor -> TBMQ -> Kafka -> ClickHouse. "
                       "Measured on insert time, so it stays honest when a sensor clock drifts.",
           thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 15}, {"color": "red", "value": 60}]), 4, 4)
b.add(stat("Ingest latency p95 (5 min)", ch("SELECT quantile(0.95)(ingest_latency_ms) / 1000 AS p95_seconds\n"
                                            "FROM iot.detections\nWHERE ts >= now() - INTERVAL 5 MINUTE", "table"),
           unit="s", decimals=2, description="TBMQ receive time -> ClickHouse insert (iot.detections.ingest_latency_ms).",
           thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 5}, {"color": "red", "value": 30}]), 4, 4)
b.add(stat("Detections stored (5 min)", ch("SELECT count() AS rows\nFROM iot.detections\nWHERE ts >= now() - INTERVAL 5 MINUTE", "table")), 4, 4)
b.add(stat("Simulator published/s", prom(f'sum(rate(tunnel_sim_published_total{{job="simulator"}}[{RATE}]))'), unit="reqps",
           description="Only while the simulator runs (make sim-up); it lives on the host, outside the cluster."), 4, 4)
b.add(stat("Simulator dropped/s", prom(f'sum(rate(tunnel_sim_dropped_total{{job="simulator"}}[{RATE}])) + sum(rate(tunnel_sim_missed_total{{job="simulator"}}[{RATE}]))'),
           unit="reqps", thresholds=ALERT_STEPS), 4, 4)
b.add(stat("Simulator scheduler lag", prom('max(tunnel_sim_max_lag_seconds{job="simulator"})'), unit="s", decimals=1,
           thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 2}, {"color": "red", "value": 10}]), 4, 4)
b.add(ts("Sent vs received (msg/s through the whole path)", [
    prom(f'sum(rate(tunnel_sim_published_total{{job="simulator"}}[{RATE}]))', "1 simulator published"),
    prom(f'sum(rate(incomingPublishMsg_published_total{{job="tbmq",statsName="totalMsgs"}}[{RATE}]))', "2 TBMQ incoming"),
    prom(f'sum(rate(integration_stats_counter_total{{job="tbmq-integration-executor",name="msgUplink",state="success"}}[{RATE}]))', "3 IE uplink"),
    prom(f'sum(rate(telemetry_processor_consumed_total[{RATE}]))', "4 processor consumed"),
    prom(f'sum(rate(telemetry_processor_produced_total[{RATE}]))', "5 processor produced"),
    prom(f'sum(rate(consensus_received_total[{RATE}]))', "6 consensus received"),
    prom(f'sum(rate(ClickHouseProfileEvents_KafkaRowsRead{{job="clickhouse"}}[{RATE}]))', "7 ClickHouse rows read"),
], unit="reqps", description="Every stage of the pipeline; a gap between two lines is where messages are lost or queued."), 12, 9)
b.add(ts("Simulator: drops, lag and clients", [
    prom(f'sum(rate(tunnel_sim_dropped_total{{job="simulator"}}[{RATE}]))', "dropped/s"),
    prom(f'sum(rate(tunnel_sim_missed_total{{job="simulator"}}[{RATE}]))', "missed/s"),
    prom(f'sum(rate(tunnel_sim_duplicates_total{{job="simulator"}}[{RATE}]))', "duplicates/s"),
    prom('max(tunnel_sim_max_lag_seconds{job="simulator"})', "max lag s"),
    prom('sum(tunnel_sim_pending_messages{job="simulator"})', "pending messages"),
    prom('sum(tunnel_sim_clients_connected{job="simulator"})', "clients connected"),
    prom('sum(tunnel_sim_clients{job="simulator"})', "clients configured"),
], legend_calcs=("lastNotNull",), overrides=[series_unit("max lag s", "s")],
   description="Rates, the scheduler's worst lag and the MQTT client count share one axis; the lag series "
               "is seconds. clients connected < clients configured means a device session dropped."), 12, 9)
b.add(table("Last 20 vehicles (iot.vehicles, last 5 min)", [
    ch("SELECT ts_exit AS time, tunnel_id, direction, lane, classification,\n"
       "  round(speed_kmh, 1) AS speed_kmh, round(length_m, 1) AS length_m, sensors, round(agreement, 2) AS agreement\n"
       "FROM iot.vehicles\nWHERE ts_entry >= now() - INTERVAL 5 MINUTE\nORDER BY ts_exit DESC\nLIMIT 20", "table")
], no_value="No vehicles fused in the last 5 min - check the simulator, or consensus calibration per tunnel.",
   description="Fixed 5-minute window on ts_entry (partition key + minmax index) and LIMIT 20: cheap at a 10s refresh. "
               "Window is on device time, so a skewed sensor clock empties this."), 12, 9)
b.add(table("Last 20 rejections (iot.detections_rejected, last 15 min)", [
    ch("SELECT processed_ts AS time, reason, tunnel_id, client_cert_cn, substring(detail, 1, 120) AS detail\n"
       "FROM iot.detections_rejected\nWHERE processed_ts >= now() - INTERVAL 15 MINUTE\nORDER BY processed_ts DESC\nLIMIT 20", "table")
], no_value="No rejections in the last 15 min - every detection was accepted.",
   description="Empty is the healthy state: rows here are payloads telemetry-processor refused."), 12, 9)

b.row("Health")
b.add(stat("Firing alerts (warning/critical)",
           prom('count(ALERTS{alertstate="firing",severity=~"critical|warning"}) or vector(0)', instant=True),
           thresholds=ALERT_STEPS), 4, 4)
b.add(stat("MQTT connections", prom('sum(connectedSessions{job="tbmq"})')), 4, 4)
b.add(stat("MQTT publishes/s", prom(f'sum(rate(incomingPublishMsg_published_total{{job="tbmq",statsName="totalMsgs"}}[{RATE}]))'),
           unit="reqps"), 4, 4)
b.add(stat("Detections produced/s", prom(f'sum(rate(telemetry_processor_produced_total[{RATE}]))'), unit="reqps"), 4, 4)
b.add(stat("Vehicles/s", prom(f'sum(rate(consensus_vehicles_total[{RATE}]))'), unit="reqps"), 4, 4)
b.add(stat("ClickHouse rows inserted/s", prom(f'sum(rate(ClickHouseProfileEvents_InsertedRows{{job="clickhouse"}}[{RATE}]))'),
           unit="reqps"), 4, 4)

b.row("MQTT ingress (TBMQ)")
b.add(ts("MQTT connections by broker pod", [prom('sum by (pod) (connectedSessions{job="tbmq"})', "{{pod}}")]), 8, 8)
b.add(ts("Connection events/s", [
    prom(f'sum(rate(connectionAccepted_total{{job="tbmq"}}[{RATE}]))', "accepted"),
    prom(f'sum(rate(connectionRefused_total{{job="tbmq"}}[{RATE}]))', "refused"),
    prom(f'sum(rate(connectionError_total{{job="tbmq"}}[{RATE}]))', "error"),
    prom(f'sum(rate(clientDisconnects_total{{job="tbmq"}}[{RATE}]))', "disconnects"),
], unit="reqps", description="Reconnect storms show as accepted ~ disconnects well above zero."), 8, 8)
b.add(ts("Incoming publishes/s", [
    prom(f'sum by (statsName) (rate(incomingPublishMsg_published_total{{job="tbmq"}}[{RATE}]))', "{{statsName}}"),
    prom(f'sum(rate(droppedMsgs_total{{job="tbmq"}}[{RATE}]))', "dropped"),
], unit="reqps"), 8, 8)

b.row("Integration executor -> app Kafka")
b.add(ts("IE uplink messages/s", [
    prom(f'sum by (state) (rate(integration_stats_counter_total{{job="tbmq-integration-executor",name="msgUplink"}}[{RATE}]))', "{{state}}"),
], unit="reqps"), 8, 8)
b.add(ts("IE lag (TBMQ internal Kafka)", [
    prom('sum by (consumergroup) (clamp_min(kafka_consumergroup_lag{job="kafka-exporter",namespace="thingsboard-mqtt-broker",consumergroup=~"ie-msg-consumer-group-.+"}, 0))', "{{consumergroup}}"),
], description="KEDA scales the integration executors on this lag."), 8, 8)
b.add(ts("App Kafka messages in/s by topic", [
    prom(f'sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",namespace="iot-pipeline",topic=~"iot\\\\..+"}}[{RATE}]))', "{{topic}}"),
], unit="reqps"), 8, 8)

b.row("Consumer lag and autoscaling")
b.add(ts("App Kafka consumer lag", [
    # consensus commits the offset of the oldest vehicle still in transit, so its consumer-group lag
    # is a transit window (minutes of detections), not a backlog: its own unread gauge belongs here.
    prom('sum by (consumergroup, topic) (clamp_min(kafka_consumergroup_lag{job="kafka-exporter",namespace="iot-pipeline",consumergroup!="consensus"}, 0))', "{{consumergroup}} / {{topic}}"),
    prom('sum(consensus_kafka_lag_records)', "consensus / unread detections"),
]), 12, 8)
b.add(ts("Replicas (KEDA)", [
    prom('kube_deployment_status_replicas{namespace="iot-pipeline",deployment=~"telemetry-processor|consensus"}', "{{deployment}}"),
    prom('kube_statefulset_status_replicas{namespace="thingsboard-mqtt-broker",statefulset="tbmq-integration-executor"}', "{{statefulset}}"),
    prom('kube_horizontalpodautoscaler_spec_max_replicas{horizontalpodautoscaler=~"keda-hpa-.+"}', "max {{horizontalpodautoscaler}}"),
], legend_calcs=("lastNotNull",)), 6, 8)
b.add(ts("KEDA scaler metric / errors", [
    prom('sum by (scaledObject) (keda_scaler_metrics_value{job="keda-operator"})', "{{scaledObject}}"),
    prom(f'sum by (scaledObject) (rate(keda_scaler_detail_errors_total{{job="keda-operator"}}[{RATE}]))', "errors/s {{scaledObject}}"),
]), 6, 8)

b.row("telemetry-processor")
b.add(ts("Throughput", [
    prom(f'sum(rate(telemetry_processor_consumed_total[{RATE}]))', "consumed"),
    prom(f'sum(rate(telemetry_processor_produced_total[{RATE}]))', "produced"),
    prom(f'sum(rate(telemetry_processor_rejected_total[{RATE}]))', "rejected"),
], unit="reqps"), 8, 8)
b.add(ts("Rejections by reason", [
    prom(f'sum by (reason) (rate(telemetry_processor_rejected_total[{RATE}]))', "{{reason}}"),
], unit="reqps", stack=True, no_value="No rejections in this range."), 8, 8)
b.add(ts("Latency", [
    prom(f'histogram_quantile(0.95, sum by (le) (rate(telemetry_processor_batch_ack_seconds_bucket[{RATE}])))', "batch ack p95"),
    prom(f'histogram_quantile(0.5, sum by (le) (rate(telemetry_processor_batch_ack_seconds_bucket[{RATE}])))', "batch ack p50"),
    prom(f'histogram_quantile(0.95, sum by (le) (rate(telemetry_processor_event_age_seconds_bucket[{RATE}])))', "event age p95"),
], unit="s", description="batch ack = poll until every record is acknowledged; event age = processed_ts - sensor ts."), 8, 8)

b.row("consensus")
b.add(ts("Throughput", [
    prom(f'sum(rate(consensus_received_total[{RATE}]))', "detections received"),
    prom(f'sum(rate(consensus_vehicles_total[{RATE}]))', "vehicles"),
    prom(f'sum(rate(consensus_duplicates_total[{RATE}]))', "duplicates"),
    prom(f'sum(rate(consensus_lost_total[{RATE}]))', "lost"),
    prom(f'sum(rate(consensus_dropped_total[{RATE}]))', "dropped"),
], unit="reqps"), 8, 8)
b.add(ts("Event age", [
    prom(f'histogram_quantile(0.95, sum by (le) (rate(consensus_event_age_seconds_bucket[{RATE}])))', "p95"),
    prom(f'histogram_quantile(0.5, sum by (le) (rate(consensus_event_age_seconds_bucket[{RATE}])))', "p50"),
], unit="s"), 8, 8)
b.add(ts("State", [
    prom('sum(consensus_tunnels)', "tunnels"),
    prom('sum(consensus_tunnels_calibrated)', "calibrated"),
    prom('sum(consensus_pending)', "pending detections"),
    prom('sum(consensus_ready)', "ready pods"),
], legend_calcs=("lastNotNull",)), 8, 8)

b.row("ClickHouse")
b.add(ts("Rows/s", [
    prom(f'sum(rate(ClickHouseProfileEvents_InsertedRows{{job="clickhouse"}}[{RATE}]))', "inserted rows"),
    prom(f'sum(rate(ClickHouseProfileEvents_KafkaRowsRead{{job="clickhouse"}}[{RATE}]))', "Kafka rows read"),
    prom(f'sum(rate(ClickHouseProfileEvents_KafkaRowsRejected{{job="clickhouse"}}[{RATE}]))', "Kafka rows rejected"),
], unit="reqps"), 8, 8)
b.add(ts("ClickHouse consumer lag", [
    prom('sum by (consumergroup) (clamp_min(kafka_consumergroup_lag{job="kafka-exporter",namespace="iot-pipeline",consumergroup=~"clickhouse-.+"}, 0))', "{{consumergroup}}"),
]), 8, 8)
b.add(ts("End-to-end latency (iot.detections)", [
    ch("SELECT $__timeInterval(ts) AS time,\n"
       "  quantile(0.95)(mqtt_latency_ms) / 1000 AS mqtt_p95,\n"
       "  quantile(0.95)(process_latency_ms) / 1000 AS processor_p95,\n"
       "  quantile(0.95)(ingest_latency_ms) / 1000 AS clickhouse_p95\n"
       "FROM iot.detections\nWHERE $__timeFilter(ts)\nGROUP BY time\nORDER BY time"),
], unit="s", description="sensor -> TBMQ, TBMQ -> telemetry-processor, TBMQ -> ClickHouse insert (ClickHouse SQL)."), 8, 8)

b.row("Logs")
b.add(logs("Pipeline errors (VictoriaLogs)",
           'kubernetes.pod_namespace:in("iot-pipeline","thingsboard-mqtt-broker","iot-storage") AND (i(error) OR i(exception) OR i(traceback))',
           no_value="No errors, exceptions or tracebacks in this range."),
      24, 10)
BOARDS = [b]

# =========================================================================================================
# Kafka (both Strimzi clusters)
# =========================================================================================================
kvar = {"type": "query", "name": "kafka_cluster", "label": "Kafka cluster", "datasource": VM,
        "query": {"query": 'label_values(up{job="kafka"}, strimzi_io_cluster)', "refId": "kafka_cluster"},
        "definition": 'label_values(up{job="kafka"}, strimzi_io_cluster)', "refresh": 2, "multi": True,
        "includeAll": True, "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "sort": 1}
K = 'strimzi_io_cluster=~"$kafka_cluster"'
b = Board("iot-kafka", "Kafka", ["iot", "kafka"],
          "Strimzi clusters tbmq-kafka (TBMQ internal) and app-kafka (pipeline): JMX exporter + Kafka Exporter.",
          variables=[kvar])
b.row("Cluster health")
b.add(stat("Brokers up", prom(f'sum by (strimzi_io_cluster) (up{{job="kafka",{K}}})', "{{strimzi_io_cluster}}")), 6, 4)
b.add(stat("Under-replicated partitions", prom(f'sum by (strimzi_io_cluster) (kafka_server_replicamanager_underreplicatedpartitions{{{K}}})', "{{strimzi_io_cluster}}"),
           thresholds=RED), 6, 4)
b.add(stat("Offline partitions", prom(f'sum by (strimzi_io_cluster) (kafka_controller_kafkacontroller_offlinepartitionscount{{{K}}})', "{{strimzi_io_cluster}}"),
           thresholds=RED), 6, 4)
b.add(stat("Active controllers", prom(f'sum by (strimzi_io_cluster) (kafka_controller_kafkacontroller_activecontrollercount{{{K}}})', "{{strimzi_io_cluster}}"),
           thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}, {"color": "red", "value": 2}]), 6, 4)
b.row("Throughput")
b.add(ts("Messages in/s", [prom(f'sum by (strimzi_io_cluster) (rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",topic="",{K}}}[{RATE}]))', "{{strimzi_io_cluster}}")],
         unit="reqps"), 8, 8)
b.add(ts("Bytes in/out", [
    prom(f'sum by (strimzi_io_cluster) (rate(kafka_server_brokertopicmetrics_bytesin_total{{job="kafka",topic="",{K}}}[{RATE}]))', "in {{strimzi_io_cluster}}"),
    prom(f'sum by (strimzi_io_cluster) (rate(kafka_server_brokertopicmetrics_bytesout_total{{job="kafka",topic="",{K}}}[{RATE}]))', "out {{strimzi_io_cluster}}"),
], unit="Bps"), 8, 8)
b.add(ts("Top topics by messages in/s", [prom(f'topk(10, sum by (strimzi_io_cluster, topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",topic!="",{K}}}[{RATE}])))', "{{strimzi_io_cluster}} {{topic}}")],
         unit="reqps"), 8, 8)
b.row("Latency and requests")
b.add(ts("Request total time p99", [prom(f'max by (strimzi_io_cluster, request) (kafka_network_requestmetrics_totaltimems{{job="kafka",quantile="0.99",{K}}})', "{{strimzi_io_cluster}} {{request}}")],
         unit="ms"), 8, 8)
b.add(ts("Requests/s", [prom(f'sum by (strimzi_io_cluster, request) (rate(kafka_network_requestmetrics_requests_total{{job="kafka",request=~"Produce|Fetch|FetchConsumer|OffsetCommit|Metadata",{K}}}[{RATE}]))', "{{strimzi_io_cluster}} {{request}}")],
         unit="reqps"), 8, 8)
b.add(ts("ISR shrinks / expands", [
    prom(f'sum by (strimzi_io_cluster) (increase(kafka_server_replicamanager_isrshrinks_total{{{K}}}[5m]))', "shrinks {{strimzi_io_cluster}}"),
    prom(f'sum by (strimzi_io_cluster) (increase(kafka_server_replicamanager_isrexpands_total{{{K}}}[5m]))', "expands {{strimzi_io_cluster}}"),
]), 8, 8)
b.row("Consumer groups (Kafka Exporter)")
b.add(ts("Lag by consumer group", [
    prom(f'sum by (strimzi_io_cluster, consumergroup) (clamp_min(kafka_consumergroup_lag{{job="kafka-exporter",{K},consumergroup!="consensus"}}, 0))', "{{strimzi_io_cluster}} {{consumergroup}}"),
    prom('sum(consensus_kafka_lag_records)', "app-kafka consensus (unread detections)")],
    description="consensus is shown by its own unread-detections gauge: its committed offset trails by "
                "design, so its consumer-group lag measures the in-transit window, not a backlog."),
      12, 9)
b.add(table("Lag by group and topic", [prom(f'sum by (strimzi_io_cluster, consumergroup, topic) (clamp_min(kafka_consumergroup_lag{{job="kafka-exporter",{K},consumergroup!="consensus"}}, 0))', instant=True)],
            transformations=[{"id": "organize", "options": {"excludeByName": {"Time": True}, "renameByName": {"Value": "lag"}}},
                             {"id": "sortBy", "options": {"sort": [{"field": "lag", "desc": True}]}}]), 12, 9)
b.add(ts("Consumer group members", [prom(f'sum by (strimzi_io_cluster, consumergroup) (kafka_consumergroup_members{{job="kafka-exporter",{K}}})', "{{strimzi_io_cluster}} {{consumergroup}}")],
         legend_calcs=("lastNotNull",)), 12, 8)
b.add(ts("Committed offset rate (msgs/s consumed)", [prom(f'sum by (strimzi_io_cluster, consumergroup) (rate(kafka_consumergroup_current_offset_sum{{job="kafka-exporter",{K}}}[{RATE}]))', "{{strimzi_io_cluster}} {{consumergroup}}")],
         unit="reqps"), 12, 8)
b.row("Storage and JVM")
b.add(ts("Log size by topic", [prom(f'topk(15, sum by (strimzi_io_cluster, topic) (kafka_log_log_size{{job="kafka",{K}}}))', "{{strimzi_io_cluster}} {{topic}}")],
         unit="bytes"), 8, 8)
b.add(ts("JVM heap used / max", [
    prom(f'sum by (pod) (jvm_memory_used_bytes{{job="kafka",area="heap",{K}}})', "used {{pod}}"),
    prom(f'sum by (pod) (jvm_memory_max_bytes{{job="kafka",area="heap",{K}}} > 0)', "max {{pod}}"),
], unit="bytes"), 8, 8)
b.add(ts("Connections by listener", [prom(f'sum by (strimzi_io_cluster, listener) (kafka_server_socket_server_metrics_connection_count{{job="kafka",{K}}})', "{{strimzi_io_cluster}} {{listener}}")]),
      8, 8)
BOARDS.append(b)

# =========================================================================================================
# TBMQ
# =========================================================================================================
b = Board("iot-tbmq", "TBMQ", ["iot", "tbmq"],
          "TBMQ 2.4.0 broker and integration executor (actuator /actuator/prometheus), Valkey, CloudNativePG.")
b.row("Sessions")
b.add(stat("Live connections", prom('sum(connectedSessions{job="tbmq"})')), 4, 4)
b.add(stat("TLS connections", prom('sum(connectedSslSessions{job="tbmq"})')), 4, 4)
b.add(stat("Client sessions (cluster, incl. offline)", prom('max(allClientSessions{job="tbmq"})')), 4, 4)
b.add(stat("Subscriptions", prom('max(subscriptions{job="tbmq"})')), 4, 4)
b.add(stat("Retained messages", prom('max(retainedMessages{job="tbmq"})')), 4, 4)
b.add(stat("Non-writable clients", prom('sum(nonWritableClients{job="tbmq"})'), thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}]), 4, 4)
b.add(ts("Connections by pod", [prom('sum by (pod) (connectedSessions{job="tbmq"})', "{{pod}}")]), 12, 8)
b.add(ts("Connection events/s", [
    prom(f'sum by (pod) (rate(connectionAccepted_total{{job="tbmq"}}[{RATE}]))', "accepted {{pod}}"),
    prom(f'sum by (pod) (rate(connectionRefused_total{{job="tbmq"}}[{RATE}]))', "refused {{pod}}"),
    prom(f'sum by (pod) (rate(connectionError_total{{job="tbmq"}}[{RATE}]))', "error {{pod}}"),
    prom(f'sum by (pod) (rate(clientDisconnects_total{{job="tbmq"}}[{RATE}]))', "disconnects {{pod}}"),
], unit="reqps"), 12, 8)
b.row("Messages")
b.add(ts("Incoming publishes -> Kafka", [prom(f'sum by (statsName) (rate(incomingPublishMsg_published_total{{job="tbmq"}}[{RATE}]))', "{{statsName}}")],
         unit="reqps"), 8, 8)
b.add(ts("Publishes consumed from Kafka", [prom(f'sum by (statsName) (rate(incomingPublishMsg_consumed_total{{job="tbmq"}}[{RATE}]))', "{{statsName}}")],
         unit="reqps"), 8, 8)
b.add(ts("Drops", [
    prom(f'sum by (pod) (rate(droppedMsgs_total{{job="tbmq"}}[{RATE}]))', "dropped {{pod}}"),
    prom(f'sum by (statsName) (rate(flowControl_total{{job="tbmq"}}[{RATE}]))', "flow control {{statsName}}"),
    prom(f'sum(rate(droppedLifecycleEvents_total{{job="tbmq"}}[{RATE}]))', "lifecycle events"),
], unit="reqps"), 8, 8)
b.row("Latency")
b.add(ts("Inbound PUBLISH in the client actor (avg)", [
    prom(f'sum by (pod) (rate(clientActor_processing_time_seconds_sum{{job="tbmq",msgType="MQTT_PUBLISH_MSG"}}[{RATE}])) / sum by (pod) (rate(clientActor_processing_time_seconds_count{{job="tbmq",msgType="MQTT_PUBLISH_MSG"}}[{RATE}]))', "processing {{pod}}"),
    prom(f'sum by (pod) (rate(clientActor_msgInQueueTime_seconds_sum{{job="tbmq"}}[{RATE}])) / sum by (pod) (rate(clientActor_msgInQueueTime_seconds_count{{job="tbmq"}}[{RATE}]))', "mailbox wait {{pod}}"),
], unit="s", description="The devices only publish, so this is the broker's hot path: how long a PUBLISH waits "
                         "in the client actor's mailbox and how long handling it takes."), 8, 8)
b.add(ts("Internal Kafka consume -> fan-out (avg)", [
    prom(f'sum by (pod) (rate(incomingPublishMsg_consumed_processing_time_seconds_sum{{job="tbmq"}}[{RATE}])) / sum by (pod) (rate(incomingPublishMsg_consumed_processing_time_seconds_count{{job="tbmq"}}[{RATE}]))', "per msg {{pod}}"),
    prom(f'sum by (pod) (rate(clientSessionsLookup_seconds_sum{{job="tbmq"}}[{RATE}])) / sum by (pod) (rate(clientSessionsLookup_seconds_count{{job="tbmq"}}[{RATE}]))', "session lookup {{pod}}"),
], unit="s", description="TBMQ reading tbmq.msg.all back and resolving subscribers. Replaces the subscriber-socket "
                         "and APPLICATION-ACK timers, which stay at zero here: nothing subscribes over MQTT, "
                         "the integration executor consumes from Kafka."), 8, 8)
b.add(ts("Internal Kafka producer send() (avg / max)", [
    prom(f'sum by (pod) (rate(kafkaProducer_send_seconds_sum{{job=~"tbmq|tbmq-integration-executor"}}[{RATE}])) / sum by (pod) (rate(kafkaProducer_send_seconds_count{{job=~"tbmq|tbmq-integration-executor"}}[{RATE}]))', "avg {{pod}}"),
    prom('max by (pod) (kafkaProducer_send_seconds_max{job=~"tbmq|tbmq-integration-executor"})', "max {{pod}}"),
], unit="s"), 8, 8)
b.row("JVM")
b.add(ts("Heap used / max", [
    prom('sum by (pod) (jvm_memory_used_bytes{job=~"tbmq|tbmq-integration-executor",area="heap"})', "used {{pod}}"),
    prom('sum by (pod) (jvm_memory_max_bytes{job=~"tbmq|tbmq-integration-executor",area="heap"} > 0)', "max {{pod}}"),
], unit="bytes"), 8, 8)
b.add(ts("GC pause time", [prom(f'sum by (pod) (rate(jvm_gc_pause_seconds_sum{{job=~"tbmq|tbmq-integration-executor"}}[{RATE}]))', "{{pod}}")],
         unit="percentunit"), 8, 8)
b.add(ts("CPU", [prom('max by (pod) (process_cpu_usage{job=~"tbmq|tbmq-integration-executor"})', "{{pod}}")], unit="percentunit"), 8, 8)
b.row("Integration executor")
b.add(ts("Uplink messages/s", [prom(f'sum by (pod, state) (rate(integration_stats_counter_total{{job="tbmq-integration-executor",name="msgUplink"}}[{RATE}]))', "{{pod}} {{state}}")],
         unit="reqps"), 8, 8)
b.add(ts("Integration processor", [prom(f'sum by (statsName) (rate(integrationProcessor_total{{job="tbmq-integration-executor"}}[{RATE}]))', "{{statsName}}")],
         unit="reqps"), 8, 8)
b.add(ts("TBMQ internal Kafka lag", [prom('topk(10, sum by (consumergroup) (clamp_min(kafka_consumergroup_lag{job="kafka-exporter",namespace="thingsboard-mqtt-broker"}, 0)))', "{{consumergroup}}")]),
      8, 8)
b.row("Valkey and PostgreSQL")
b.add(ts("Valkey memory", [
    prom('max by (pod) (redis_memory_used_bytes{job="valkey"})', "used {{pod}}"),
    prom('max by (pod) (kube_pod_container_resource_limits{namespace="thingsboard-mqtt-broker",container="valkey",resource="memory"})', "container limit"),
], unit="bytes"), 6, 8)
b.add(ts("Valkey clients / commands", [
    prom('sum(redis_connected_clients{job="valkey"})', "clients"),
    prom(f'sum(rate(redis_commands_processed_total{{job="valkey"}}[{RATE}]))', "commands/s"),
], overrides=[series_unit("commands/s", "reqps")]), 6, 8)
b.add(ts("PostgreSQL backends / database size", [
    prom('sum by (pod) (cnpg_backends_total{job="cnpg"})', "backends {{pod}}"),
    prom('max by (datname) (cnpg_pg_database_size_bytes{job="cnpg",datname="thingsboard_mqtt_broker"})', "db size"),
], overrides=[series_unit("db size", "bytes")]), 6, 8)
b.add(ts("PostgreSQL role / replication lag", [
    prom('max by (pod) (cnpg_pg_replication_in_recovery{job="cnpg"})', "in recovery {{pod}}"),
    prom('max by (pod) (cnpg_pg_replication_lag{job="cnpg"})', "lag s {{pod}}"),
    prom('max by (pod) (cnpg_pg_replication_streaming_replicas{job="cnpg"})', "streaming replicas {{pod}}"),
], legend_calcs=("lastNotNull",)), 6, 8)
b.row("Logs")
b.add(logs("TBMQ warnings and errors (VictoriaLogs)",
           'kubernetes.pod_namespace:="thingsboard-mqtt-broker" AND kubernetes.container_name:="server" AND (WARN OR ERROR)',
           no_value="No warnings or errors from the brokers in this range - widen the time picker to see older ones.",
           description="Broker log lines only. Quiet here is the healthy state."), 24, 10)
BOARDS.append(b)

# =========================================================================================================
# Tunnel traffic (ClickHouse, database iot)
# =========================================================================================================
# The profile tunnels are the configured ones; generator-mode tunnels only exist in the data.
TUNNEL_QUERY = ("SELECT tunnel_id FROM iot.tunnel_profiles FINAL ORDER BY tunnel_id\n"
                "UNION DISTINCT\n"
                "SELECT DISTINCT tunnel_id FROM iot.traffic WHERE ts > now() - INTERVAL 1 DAY LIMIT 2000")
tvar = {"type": "query", "name": "tunnel", "label": "Tunnel", "datasource": CH,
        "query": {"refId": "tunnel", "queryType": "sql",
                  "rawSql": TUNNEL_QUERY},
        "definition": TUNNEL_QUERY,
        "refresh": 2, "multi": True, "includeAll": True, "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "sort": 1}
T = "tunnel_id IN (${tunnel:singlequote})"
b = Board("iot-tunnel-traffic", "Tunnel traffic", ["iot", "clickhouse"],
          "Vehicles, speeds and device health from ClickHouse database iot (tables vehicles_1m, vehicles, traffic, sensor_health, detections). Ten Turkish vehicle types; health is what each device reports about itself.",
          variables=[tvar], refresh="30s", time_from="now-3h")
b.row("Traffic")
b.add(stat("Vehicles in range", ch(f"SELECT sum(vehicles) AS vehicles\nFROM iot.vehicles_1m\nWHERE $__timeFilter(minute) AND {T}", "table")), 6, 4)
b.add(stat("Heavy vehicle share", ch(f"SELECT sumIf(vehicles, classification IN ('KAMYON', 'CEKICI_YARI_ROMORK', 'OTOBUS')) / greatest(sum(vehicles), 1) AS heavy\nFROM iot.vehicles_1m\nWHERE $__timeFilter(minute) AND {T}", "table"),
           unit="percentunit", decimals=1), 6, 4)
b.add(stat("Average speed", ch(f"SELECT avgMerge(speed_avg) AS avg_speed\nFROM iot.vehicles_1m\nWHERE $__timeFilter(minute) AND {T}", "table"),
           unit="velocitykmh", decimals=1), 6, 4)
b.add(stat("Tunnels reporting (last 10 min)", ch(f"SELECT uniqExact(tunnel_id) AS tunnels\nFROM iot.traffic\nWHERE ts > now() - INTERVAL 10 MINUTE AND {T}", "table")), 6, 4)
b.add(ts("Vehicles per interval by type", [
    ch(f"SELECT $__timeInterval(minute) AS time, classification, sum(vehicles) AS vehicles\n"
       f"FROM iot.vehicles_1m\nWHERE $__timeFilter(minute) AND {T}\nGROUP BY time, classification\nORDER BY time")
], stack=True), 12, 8)
b.add(ts("Average speed by direction", [
    ch(f"SELECT $__timeInterval(minute) AS time, direction, avgMerge(speed_avg) AS speed\n"
       f"FROM iot.vehicles_1m\nWHERE $__timeFilter(minute) AND {T}\nGROUP BY time, direction\nORDER BY time")
], unit="velocitykmh"), 12, 8)
b.add(table("Vehicles per tunnel and type", [
    ch(f"SELECT tunnel_id,\n  sumIf(vehicles, classification = 'OTOMOBIL') AS otomobil,\n"
       f"  sumIf(vehicles, classification IN ('HAFIF_TICARI', 'MINIBUS')) AS hafif,\n"
       f"  sumIf(vehicles, classification IN ('KAMYON', 'CEKICI_YARI_ROMORK', 'OTOBUS')) AS agir,\n"
       f"  sumIf(vehicles, classification IN ('MOTOSIKLET', 'TRAKTOR', 'OZEL_AMACLI_TASIT')) AS diger,\n"
       f"  sumIf(vehicles, classification = 'BILINMEYEN') AS bilinmeyen,\n"
       f"  sum(vehicles) AS total,\n  round(avgMerge(speed_avg), 1) AS avg_speed_kmh,\n  round(max(speed_max), 1) AS max_speed_kmh\n"
       f"FROM iot.vehicles_1m\nWHERE $__timeFilter(minute) AND {T}\nGROUP BY tunnel_id\nORDER BY total DESC\nLIMIT 50", "table")
]), 12, 10)
b.add(ts("Traffic windows by direction (iot.traffic)", [
    ch(f"SELECT $__timeInterval(ts) AS time,\n  sum(arraySum(mapValues(start_to_end))) AS start_to_end,\n"
       f"  sum(arraySum(mapValues(end_to_start))) AS end_to_start\n"
       f"FROM iot.traffic\nWHERE $__timeFilter(ts) AND {T}\nGROUP BY time\nORDER BY time")
]), 12, 10)
b.add(ts("Speed distribution (iot.vehicles)", [
    ch(f"SELECT $__timeInterval(ts_entry) AS time,\n  quantile(0.5)(speed_kmh) AS p50,\n  quantile(0.85)(speed_kmh) AS p85,\n  quantile(0.95)(speed_kmh) AS p95\n"
       f"FROM iot.vehicles\nWHERE $__timeFilter(ts_entry) AND {T}\nGROUP BY time\nORDER BY time")
], unit="velocitykmh"), 12, 8)
b.add(ts("Consensus quality (iot.vehicles)", [
    ch(f"SELECT $__timeInterval(ts_entry) AS time,\n  avg(agreement) AS agreement,\n  avg(confidence) AS confidence,\n  countIf(sensors < 3) / count() AS incomplete_share\n"
       f"FROM iot.vehicles\nWHERE $__timeFilter(ts_entry) AND {T}\nGROUP BY time\nORDER BY time")
], unit="percentunit"), 12, 8)
b.row("Device health (reported by the devices)")
b.add(table("Devices by latest status (last 10 min)", [
    ch(f"SELECT status, count() AS sensors\nFROM (\n  SELECT sensor_id, argMax(status, ts) AS status\n  FROM iot.sensor_health\n"
       f"  WHERE ts > now() - INTERVAL 10 MINUTE AND {T}\n  GROUP BY sensor_id\n)\nGROUP BY status\nORDER BY status", "table")
], no_value="No device reported in the last 10 min - the devices are down, or their clocks are skewed.",
   description="Counts the 3 devices per tunnel by their latest self-reported status. "
               "The window is on device time, so a skewed sensor clock empties this."), 6, 9)
b.add(ts("Miss rate by position", [
    ch(f"SELECT $__timeInterval(ts) AS time, position,\n"
       f"  sum(missed) / greatest(sum(missed + detections), 1) AS miss_rate\n"
       f"FROM iot.sensor_health\nWHERE $__timeFilter(ts) AND {T}\nGROUP BY time, position\nORDER BY time")
], unit="percentunit"), 9, 9)
b.add(ts("Missed detections and duplicates", [
    ch(f"SELECT $__timeInterval(ts) AS time, sum(missed) AS missed, sum(duplicates) AS duplicates\n"
       f"FROM iot.sensor_health\nWHERE $__timeFilter(ts) AND {T}\nGROUP BY time\nORDER BY time")
]), 9, 9)
b.add(table("Unhealthy devices (latest report in range)", [
    ch(f"SELECT sensor_id, any(tunnel_id) AS tunnel, argMax(device_id, ts) AS device_id,\n"
       f"  argMax(device_serial, ts) AS serial, any(position) AS position,\n  argMax(status, ts) AS status,\n"
       f"  argMax(missed, ts) AS missed,\n  argMax(duplicates, ts) AS duplicates,\n"
       f"  argMax(detections, ts) AS detections,\n  max(ts) AS last_report\n"
       f"FROM iot.sensor_health\nWHERE $__timeFilter(ts) AND {T}\nGROUP BY sensor_id\nHAVING status != 'ok'\nORDER BY missed DESC\nLIMIT 100", "table")
], no_value="Every device that reported in this range reported ok.",
   description="Devices self-report their condition; empty means all of them are healthy."), 24, 10)
b.row("Data quality")
b.add(ts("Rejected detections by reason (iot.detections_rejected)", [
    ch("SELECT $__timeInterval(processed_ts) AS time, reason, count() AS rejected\n"
       "FROM iot.detections_rejected\nWHERE $__timeFilter(processed_ts)\nGROUP BY time, reason\nORDER BY time")
], stack=True, no_value="No rejections in this range - every detection was accepted."), 12, 8)
b.add(ts("Pipeline latency p95 (iot.detections)", [
    ch(f"SELECT $__timeInterval(ts) AS time,\n  quantile(0.95)(mqtt_latency_ms) / 1000 AS sensor_to_tbmq,\n"
       f"  quantile(0.95)(process_latency_ms) / 1000 AS tbmq_to_processor,\n  quantile(0.95)(ingest_latency_ms) / 1000 AS tbmq_to_clickhouse\n"
       f"FROM iot.detections\nWHERE $__timeFilter(ts) AND {T}\nGROUP BY time\nORDER BY time")
], unit="s"), 12, 8)
BOARDS.append(b)

# =========================================================================================================
# Pipeline economics: per component, what it consumes, what it moves, what it costs per message
# =========================================================================================================
# Every query on this board was run against the live VMSingle before it was written down. Where a thing is
# not measurable (CPU attributed to TLS, per-topic CPU inside a broker) the panel is left out rather than
# faked; the honest proxies that do exist are in the "TLS" row and labelled as proxies.
MSGS = f'clamp_min(sum(rate(incomingPublishMsg_published_total{{job="tbmq",statsName="totalMsgs"}}[{RATE}])), 1)'
PIPE_NS = "thingsboard-mqtt-broker|iot-pipeline|iot-storage"
MC = "suffix: mc/(msg/s)"  # millicores of CPU per msg/s of pipeline throughput

# (legend, namespace regex, pod regex) -- in message-path order, then the supporting services.
STAGES = [
    ("1 TBMQ broker", "thingsboard-mqtt-broker", "tbmq-[0-9]+"),
    ("2 TBMQ internal Kafka", "thingsboard-mqtt-broker", "tbmq-kafka-dual-role-.*"),
    ("3 integration executor", "thingsboard-mqtt-broker", "tbmq-integration-executor-.*"),
    ("4 app Kafka", "iot-pipeline", "app-kafka-dual-role-.*"),
    ("5 telemetry-processor", "iot-pipeline", "telemetry-processor-.*"),
    ("6 consensus", "iot-pipeline", "consensus-.*"),
    ("7 ClickHouse", "iot-storage", "clickhouse-.*"),
    ("8 SeaweedFS", "iot-storage", "seaweedfs-.*"),
    ("9 PostgreSQL", "thingsboard-mqtt-broker", "tbmq-db-.*"),
    ("10 Valkey", "thingsboard-mqtt-broker", "tbmq-valkey-.*"),
    ("11 Kafka exporters", "iot-pipeline|thingsboard-mqtt-broker", ".*-kafka-exporter-.*"),
    ("12 Strimzi operators", "strimzi-system|iot-pipeline|thingsboard-mqtt-broker",
     "strimzi-cluster-operator-.*|.*-entity-operator-.*"),
    ("13 observability", "monitoring", ".+"),
]


def stage_cpu(ns, pods):
    return f'sum(rate(container_cpu_usage_seconds_total{{namespace=~"{ns}",pod=~"{pods}",container!="",image!=""}}[{RATE}]))'


def stage_mem(ns, pods):
    return f'sum(container_memory_working_set_bytes{{namespace=~"{ns}",pod=~"{pods}",container!="",image!=""}})'


def stage_cost(ns, pods):
    return f"1000 * {stage_cpu(ns, pods)} / {MSGS}"


b = Board("iot-pipeline-economics", "Pipeline economics", ["iot", "pipeline", "cost"],
          "Per component: what it consumes (CPU, memory), what it moves (msg/s per hop and the deltas between "
          "hops), how long it takes (latency per hop) and what it costs (millicores of CPU per msg/s of "
          "pipeline throughput). The headline row divides each stage's CPU by the TBMQ incoming publish rate, "
          "so a stage that is expensive per message stands out regardless of load. "
          "TLS CAVEAT: nothing in this cluster attributes CPU to TLS -- neither the kernel, the JVM nor "
          "cAdvisor separates encryption from the rest of a process. The TLS row therefore shows only "
          "proxies (how many sessions are TLS, handshake/accept rate, bytes moved over those sockets); "
          "no panel here claims to measure the CPU cost of TLS, and none should be read that way.",
          refresh="30s")

b.row("Cost per message (headline)")
b.add(stat("Pipeline message rate", prom(f'sum(rate(incomingPublishMsg_published_total{{job="tbmq",statsName="totalMsgs"}}[{RATE}]))'),
           unit="reqps", decimals=0,
           description="TBMQ incoming publishes: the denominator of every cost-per-message panel on this board."), 4, 4)
b.add(stat("Chain CPU", prom(stage_cpu(PIPE_NS, ".+")), unit="none", decimals=2,
           description="Cores burned by everything in thingsboard-mqtt-broker + iot-pipeline + iot-storage."), 4, 4)
b.add(stat("Chain cost per message", prom(f"1000 * {stage_cpu(PIPE_NS, '.+')} / {MSGS}"), unit=MC, decimals=3,
           description="Millicores per msg/s across the whole chain. This includes the fixed overhead that does "
                       "not scale with load, so it falls as throughput rises."), 4, 4)
b.add(stat("Observability cost per message", prom(f"1000 * {stage_cpu('monitoring', '.+')} / {MSGS}"), unit=MC, decimals=3,
           description="What it costs to watch the pipeline: the whole monitoring namespace per pipeline msg/s."), 4, 4)
b.add(stat("Cost of a TBMQ broker RPC", prom(
    f'1000 * {stage_cpu("thingsboard-mqtt-broker", "tbmq-kafka-dual-role-.*")} / '
    f'clamp_min(sum(rate(kafka_network_requestmetrics_requests_total{{job="kafka",strimzi_io_cluster="tbmq-kafka",request=~"Produce|FetchConsumer|OffsetCommit"}}[{RATE}])), 1)'),
    unit="suffix: mc/(RPC/s)", decimals=3,
    description="Internal Kafka CPU divided by its Produce+Fetch+OffsetCommit rate."), 4, 4)
b.add(stat("Cost of an app Kafka RPC", prom(
    f'1000 * {stage_cpu("iot-pipeline", "app-kafka-dual-role-.*")} / '
    f'clamp_min(sum(rate(kafka_network_requestmetrics_requests_total{{job="kafka",strimzi_io_cluster="app-kafka",request=~"Produce|FetchConsumer|OffsetCommit"}}[{RATE}])), 1)'),
    unit="suffix: mc/(RPC/s)", decimals=3), 4, 4)
b.add(bars("Cost per message by stage", [prom(stage_cost(ns, pods), name, instant=True) for name, ns, pods in STAGES],
           unit=MC, decimals=3,
           description="Millicores of CPU per msg/s of pipeline throughput, per stage. The longest bar is where "
                       "the pipeline burns its budget. Stages 11-13 are overhead, not message path."), 12, 12)
b.add(ts("Cost per message by stage over time", [prom(stage_cost(ns, pods), name) for name, ns, pods in STAGES],
         unit=MC, stack=True,
         description="Stacked: the top of the stack is the chain cost per message. A stage whose band widens "
                     "while the others hold is getting less efficient, not just busier."), 12, 12)

b.row("Resource per component")
b.add(ts("CPU cores by stage", [prom(stage_cpu(ns, pods), name) for name, ns, pods in STAGES],
         unit="none", stack=True, description="container_cpu_usage_seconds_total, pause containers excluded."), 12, 10)
b.add(ts("Memory working set by stage", [prom(stage_mem(ns, pods), name) for name, ns, pods in STAGES],
         unit="bytes", stack=True), 12, 10)
b.add(table("Memory vs its limit, per pod", [
    prom(f'sum by (pod) (container_memory_working_set_bytes{{namespace=~"{PIPE_NS}",container!="",image!=""}})', instant=True),
    prom(f'sum by (pod) (kube_pod_container_resource_limits{{namespace=~"{PIPE_NS}",resource="memory"}})', instant=True),
    prom(f'sum by (pod) (container_memory_working_set_bytes{{namespace=~"{PIPE_NS}",container!="",image!=""}}) / '
         f'clamp_min(sum by (pod) (kube_pod_container_resource_limits{{namespace=~"{PIPE_NS}",resource="memory"}}), 1)', instant=True),
], description="A pod sitting near 1.0 is one OOMKill away from a restart.",
    transformations=[{"id": "joinByField", "options": {"byField": "pod", "mode": "outer"}},
                     {"id": "organize", "options": {"excludeByName": {"Time 1": True, "Time 2": True, "Time 3": True},
                                                    "renameByName": {"Value #A": "working set", "Value #B": "limit",
                                                                     "Value #C": "used / limit"}}},
                     {"id": "sortBy", "options": {"sort": [{"field": "used / limit", "desc": True}]}}],
    overrides=[{"matcher": {"id": "byName", "options": "working set"},
                "properties": [{"id": "unit", "value": "bytes"}]},
               {"matcher": {"id": "byName", "options": "limit"},
                "properties": [{"id": "unit", "value": "bytes"}]},
               {"matcher": {"id": "byName", "options": "used / limit"},
                "properties": [{"id": "unit", "value": "percentunit"},
                               {"id": "custom.cellOptions", "value": {"type": "gauge", "mode": "gradient"}},
                               {"id": "max", "value": 1}, {"id": "min", "value": 0}]}]), 12, 10)
b.add(ts("CPU throttling", [
    prom(f'sum by (pod) (rate(container_cpu_cfs_throttled_seconds_total{{container!=""}}[{RATE}])) > 0', "throttled s/s {{pod}}"),
    prom(f'sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{{container!=""}}[{RATE}])) / '
         f'clamp_min(sum by (pod) (rate(container_cpu_cfs_periods_total{{container!=""}}[{RATE}])), 1) > 0', "throttled share {{pod}}"),
], overrides=[series_unit("throttled share.*", "percentunit")], description="Only pods with a CPU limit can be throttled, and on this cluster that is KEDA and the Strimzi "
               "operator alone -- every pipeline pod runs without a CPU limit, so an empty chart here means "
               "'no limits configured', not 'no CPU pressure'. Node-level headroom is in the Saturation row."), 12, 10)

b.row("Throughput per hop")
b.add(ts("Messages per second at each hop", [
    prom(f'sum(rate(tunnel_sim_published_total{{job="simulator"}}[{RATE}]))', "01 simulator published"),
    prom(f'sum(rate(incomingPublishMsg_published_total{{job="tbmq",statsName="totalMsgs"}}[{RATE}]))', "02 TBMQ incoming"),
    prom(f'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="tbmq-kafka",topic="tbmq.msg.all"}}[{RATE}]))', "03 tbmq.msg.all"),
    prom(f'sum(rate(incomingPublishMsg_consumed_total{{job="tbmq",statsName="totalMsgs"}}[{RATE}]))', "04 TBMQ consumed"),
    prom(f'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="tbmq-kafka",topic=~"tbmq\\\\.msg\\\\.ie\\\\..+"}}[{RATE}]))', "05 integration topics"),
    prom(f'sum(rate(integration_stats_counter_total{{job="tbmq-integration-executor",name="msgUplink",state="success"}}[{RATE}]))', "06 IE uplink ok"),
    prom(f'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="app-kafka",topic="iot.mqtt.ingest"}}[{RATE}]))', "07 iot.mqtt.ingest"),
    prom(f'sum(rate(telemetry_processor_consumed_total[{RATE}]))', "08 processor consumed"),
    prom(f'sum(rate(telemetry_processor_produced_total[{RATE}]))', "09 processor produced"),
    prom(f'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="app-kafka",topic="iot.detections"}}[{RATE}]))', "10 iot.detections"),
    prom(f'sum(rate(consensus_received_total[{RATE}]))', "11 consensus received"),
    prom(f'sum(rate(consensus_vehicles_total[{RATE}]))', "12 consensus vehicles"),
    prom(f'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="app-kafka",topic="iot.vehicles"}}[{RATE}]))', "13 iot.vehicles"),
    prom(f'sum(rate(ClickHouseProfileEvents_KafkaRowsRead{{job="clickhouse"}}[{RATE}]))', "14 ClickHouse rows read"),
    prom(f'sum(rate(ClickHouseProfileEvents_InsertedRows{{job="clickhouse"}}[{RATE}]))', "15 ClickHouse rows inserted"),
], unit="reqps",
    description="Hops 01-11 should sit on top of each other at the sensor rate. Hops 12-13 are one vehicle per "
                "~3 detections, and 14-15 are larger because ClickHouse also reads vehicles/traffic/sensor-health "
                "and fans every row out into materialised views."), 12, 11)
b.add(ts("Delta between consecutive hops (msg/s lost or buffered)", [
    prom(f'sum(rate(tunnel_sim_published_total{{job="simulator"}}[{RATE}])) - sum(rate(incomingPublishMsg_published_total{{job="tbmq",statsName="totalMsgs"}}[{RATE}]))', "01->02 simulator to TBMQ"),
    prom(f'sum(rate(incomingPublishMsg_published_total{{job="tbmq",statsName="totalMsgs"}}[{RATE}])) - sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="tbmq-kafka",topic="tbmq.msg.all"}}[{RATE}]))', "02->03 TBMQ to tbmq.msg.all"),
    prom(f'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="tbmq-kafka",topic="tbmq.msg.all"}}[{RATE}])) - sum(rate(integration_stats_counter_total{{job="tbmq-integration-executor",name="msgUplink",state="success"}}[{RATE}]))', "03->06 fan-out to IE uplink"),
    prom(f'sum(rate(integration_stats_counter_total{{job="tbmq-integration-executor",name="msgUplink",state="success"}}[{RATE}])) - sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="app-kafka",topic="iot.mqtt.ingest"}}[{RATE}]))', "06->07 IE to iot.mqtt.ingest"),
    prom(f'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="app-kafka",topic="iot.mqtt.ingest"}}[{RATE}])) - sum(rate(telemetry_processor_consumed_total[{RATE}]))', "07->08 ingest to processor"),
    prom(f'sum(rate(telemetry_processor_consumed_total[{RATE}])) - sum(rate(telemetry_processor_produced_total[{RATE}]))', "08->09 processor in to out"),
    prom(f'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="app-kafka",topic="iot.detections"}}[{RATE}])) - sum(rate(consensus_received_total[{RATE}]))', "10->11 detections to consensus"),
], unit="reqps", min0=False,
    description="Positive = the downstream stage is behind or dropping; negative = it is catching up on a "
                "backlog. Sustained non-zero on one line localises the loss to that hop."), 12, 11)
b.add(ts("Rejected, dropped, lost and duplicate counters", [
    prom(f'sum(rate(tunnel_sim_dropped_total{{job="simulator"}}[{RATE}]))', "simulator dropped"),
    prom(f'sum(rate(tunnel_sim_missed_total{{job="simulator"}}[{RATE}]))', "simulator missed"),
    prom(f'sum(rate(tunnel_sim_duplicates_total{{job="simulator"}}[{RATE}]))', "simulator duplicates"),
    prom(f'sum(rate(droppedMsgs_total{{job="tbmq"}}[{RATE}]))', "TBMQ dropped"),
    prom(f'sum(rate(connectionRefused_total{{job="tbmq"}}[{RATE}]))', "TBMQ connections refused"),
    prom(f'sum(rate(integration_stats_counter_total{{job="tbmq-integration-executor",name="msgUplink",state="failed"}}[{RATE}]))', "IE uplink failed"),
    prom(f'sum(rate(telemetry_processor_rejected_total[{RATE}]))', "processor rejected"),
    prom(f'sum(rate(telemetry_processor_delivery_failures_total[{RATE}]))', "processor delivery failures"),
    prom(f'sum(rate(consensus_invalid_total[{RATE}]))', "consensus invalid"),
    prom(f'sum(rate(consensus_duplicates_total[{RATE}]))', "consensus duplicates"),
    prom(f'sum(rate(consensus_lost_total[{RATE}]))', "consensus lost"),
    prom(f'sum(rate(consensus_dropped_total[{RATE}]))', "consensus dropped"),
    prom(f'sum(rate(ClickHouseProfileEvents_KafkaRowsRejected{{job="clickhouse"}}[{RATE}]))', "ClickHouse rows rejected"),
    prom(f'sum(rate(ClickHouseProfileEvents_KafkaMessagesFailed{{job="clickhouse"}}[{RATE}]))', "ClickHouse messages failed"),
], unit="reqps", description="Every counter in the chain that means 'this message did not make it'. "
                             "Flat at zero is the expected picture."), 12, 11)
b.add(ts("Rejections by reason (telemetry-processor) and rejected rows", [
    prom(f'sum by (reason) (rate(telemetry_processor_rejected_total[{RATE}]))', "processor {{reason}}"),
    prom(f'sum(rate(telemetry_processor_rejected_produced_total[{RATE}]))', "written to the rejected topic"),
], unit="reqps", stack=True,
    description="telemetry-processor is the only stage that classifies why it drops a record; the rows it "
                "writes out land in iot.detections_rejected (see the Tunnel traffic board)."), 12, 11)

b.row("Latency per hop")
b.add(ts("End-to-end latency percentiles (ClickHouse, iot.detections)", [
    ch("SELECT $__timeInterval(ts) AS time,\n"
       "  quantile(0.50)(mqtt_latency_ms) / 1000 AS sensor_to_tbmq_p50,\n"
       "  quantile(0.95)(mqtt_latency_ms) / 1000 AS sensor_to_tbmq_p95,\n"
       "  quantile(0.50)(process_latency_ms) / 1000 AS tbmq_to_processor_p50,\n"
       "  quantile(0.95)(process_latency_ms) / 1000 AS tbmq_to_processor_p95,\n"
       "  quantile(0.50)(ingest_latency_ms) / 1000 AS tbmq_to_clickhouse_p50,\n"
       "  quantile(0.95)(ingest_latency_ms) / 1000 AS tbmq_to_clickhouse_p95\n"
       "FROM iot.detections\nWHERE $__timeFilter(ts)\nGROUP BY time\nORDER BY time"),
], unit="s", description="The only true per-message end-to-end timings in the system: they come from "
                         "timestamps carried on the message itself (ts, received_ts, processed_ts, inserted_at)."), 12, 10)
b.add(ts("Inside TBMQ: where the broker's time goes (avg)", [
    prom(f'sum(rate(incomingPublishMsg_consumed_pack_processing_time_seconds_sum{{job="tbmq"}}[{RATE}])) / '
         f'clamp_min(sum(rate(incomingPublishMsg_consumed_pack_processing_time_seconds_count{{job="tbmq"}}[{RATE}])), 0.001)', "pack processing"),
    prom(f'sum(rate(incomingPublishMsg_consumed_processing_time_seconds_sum{{job="tbmq"}}[{RATE}])) / '
         f'clamp_min(sum(rate(incomingPublishMsg_consumed_processing_time_seconds_count{{job="tbmq"}}[{RATE}])), 0.001)', "per message processing"),
    prom(f'sum(rate(kafkaConsumer_commit_seconds_sum{{job="tbmq"}}[{RATE}])) / '
         f'clamp_min(sum(rate(kafkaConsumer_commit_seconds_count{{job="tbmq"}}[{RATE}])), 0.001)', "commitSync"),
    prom(f'sum(rate(kafkaProducer_send_seconds_sum{{job="tbmq"}}[{RATE}])) / '
         f'clamp_min(sum(rate(kafkaProducer_send_seconds_count{{job="tbmq"}}[{RATE}])), 0.001)', "producer send"),
    prom(f'sum(rate(delivery_seconds_sum{{job="tbmq"}}[{RATE}])) / '
         f'clamp_min(sum(rate(delivery_seconds_count{{job="tbmq"}}[{RATE}])), 0.001)', "delivery to subscriber"),
    prom(f'sum(rate(clientActor_msgInQueueTime_seconds_sum{{job="tbmq"}}[{RATE}])) / '
         f'clamp_min(sum(rate(clientActor_msgInQueueTime_seconds_count{{job="tbmq"}}[{RATE}])), 0.001)', "client actor queue wait"),
], unit="s", description="Micrometer timers on TBMQ's actuator endpoint. 'commitSync' is per pack, and TBMQ "
                         "commits once per pack -- see the Kafka efficiency row for what that costs."), 12, 10)
b.add(ts("Python stages: histogram percentiles", [
    prom(f'histogram_quantile(0.95, sum by (le) (rate(telemetry_processor_batch_duration_seconds_bucket[{RATE}])))', "processor batch duration p95"),
    prom(f'histogram_quantile(0.95, sum by (le) (rate(telemetry_processor_batch_ack_seconds_bucket[{RATE}])))', "processor batch ack p95"),
    prom(f'histogram_quantile(0.50, sum by (le) (rate(telemetry_processor_event_age_seconds_bucket[{RATE}])))', "processor event age p50"),
    prom(f'histogram_quantile(0.95, sum by (le) (rate(telemetry_processor_event_age_seconds_bucket[{RATE}])))', "processor event age p95"),
    prom(f'histogram_quantile(0.50, sum by (le) (rate(consensus_event_age_seconds_bucket[{RATE}])))', "consensus event age p50"),
    prom(f'histogram_quantile(0.95, sum by (le) (rate(consensus_event_age_seconds_bucket[{RATE}])))', "consensus event age p95"),
], unit="s", description="The only histograms the Python services export (telemetry_processor: batch_duration, "
                         "batch_ack, event_age; consensus: event_age). Neither service times an individual "
                         "record end to end, so there is no per-record p99 to show."), 12, 10)
b.add(ts("Queued work: lag, backlog and in-flight records", [
    prom(f'sum by (strimzi_io_cluster, consumergroup) (clamp_min(kafka_consumergroup_lag{{job="kafka-exporter",consumergroup=~"msg-all-consumer-group|ie-msg-consumer-group-.+|telemetry-processor|clickhouse-.+"}}, 0))', "lag {{strimzi_io_cluster}}/{{consumergroup}}"),
    prom('sum(consensus_kafka_lag_records)', "consensus unread detections"),
    prom('sum(telemetry_processor_inflight_records)', "processor in-flight records"),
    prom('sum(consensus_pending)', "consensus pending detections"),
    prom('sum(consensus_out_pending)', "consensus producer queue"),
], description="Kafka Exporter lag per group, plus each service's own view of its backlog. The consensus "
               "consumer group is deliberately excluded from the exporter series: consensus manages its own "
               "offsets for replay, so the exporter's number for it is meaningless -- "
               "consensus_kafka_lag_records is the real unread count."), 12, 10)

b.row("Kafka efficiency (the TBMQ commit overhead)")
b.add(stat("TBMQ internal Kafka: RPCs per record", prom(
    f'sum(rate(kafka_network_requestmetrics_requests_total{{job="kafka",strimzi_io_cluster="tbmq-kafka",request=~"Produce|FetchConsumer|OffsetCommit"}}[{RATE}])) / '
    f'clamp_min(sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="tbmq-kafka",topic=""}}[{RATE}])), 1)'),
    unit="suffix: RPC/record", decimals=3,
    thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 0.25}, {"color": "red", "value": 0.5}],
    description="Client-facing broker calls divided by records written to the broker."), 6, 4)
b.add(stat("App Kafka: RPCs per record", prom(
    f'sum(rate(kafka_network_requestmetrics_requests_total{{job="kafka",strimzi_io_cluster="app-kafka",request=~"Produce|FetchConsumer|OffsetCommit"}}[{RATE}])) / '
    f'clamp_min(sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",strimzi_io_cluster="app-kafka",topic=""}}[{RATE}])), 1)'),
    unit="suffix: RPC/record", decimals=3,
    thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 0.25}, {"color": "red", "value": 0.5}],
    description="Same formula as the panel to its left. The gap between the two numbers is the whole story."), 6, 4)
b.add(stat("TBMQ: messages per commitSync", prom(
    f'sum(rate(incomingPublishMsg_consumed_total{{job="tbmq",statsName="successfulMsgs"}}[{RATE}])) / '
    f'clamp_min(sum(rate(kafkaConsumer_commit_seconds_count{{job="tbmq"}}[{RATE}])), 0.001)'),
    unit="suffix: msg/commit", decimals=2,
    thresholds=[{"color": "red", "value": None}, {"color": "orange", "value": 5}, {"color": "green", "value": 20}],
    description="TBMQ commits its consumer offsets once per pack, so this is also the average pack size. "
                "Small packs mean one OffsetCommit RPC and one __consumer_offsets write per handful of messages."), 6, 4)
b.add(stat("Integration executor: messages per iteration", prom(
    f'sum(rate(integrationProcessor_total{{job="tbmq-integration-executor",statsName="successfulMsgs"}}[{RATE}])) / '
    f'clamp_min(sum(rate(integrationProcessor_total{{job="tbmq-integration-executor",statsName="successfulIterations"}}[{RATE}])), 0.001)'),
    unit="suffix: msg/iteration", decimals=2,
    thresholds=[{"color": "red", "value": None}, {"color": "orange", "value": 5}, {"color": "green", "value": 20}]), 6, 4)
b.add(ts("Broker RPCs per record, both clusters", [
    prom(f'sum by (strimzi_io_cluster) (rate(kafka_network_requestmetrics_requests_total{{job="kafka",request=~"Produce|FetchConsumer|OffsetCommit"}}[{RATE}])) / '
         f'clamp_min(sum by (strimzi_io_cluster) (rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",topic=""}}[{RATE}])), 1)', "{{strimzi_io_cluster}}"),
], unit="suffix: RPC/record",
    description="tbmq-kafka pays several times what app-kafka pays for the same record, because TBMQ "
                "commitSync()s after every small pack while the pipeline producers batch."), 8, 9)
b.add(ts("Request rate by type and cluster", [
    prom(f'sum by (strimzi_io_cluster, request) (rate(kafka_network_requestmetrics_requests_total{{job="kafka",request=~"Produce|FetchConsumer|OffsetCommit|ListOffsets|Heartbeat|Metadata"}}[{RATE}]))', "{{strimzi_io_cluster}} {{request}}"),
], unit="reqps", description="The absolute RPC volume behind the ratio above."), 8, 9)
b.add(ts("__consumer_offsets write rate", [
    prom(f'sum by (strimzi_io_cluster) (rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",topic="__consumer_offsets"}}[{RATE}]))', "offset records/s {{strimzi_io_cluster}}"),
    prom(f'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",topic="__consumer_offsets"}}[{RATE}])) / {MSGS}', "offset records per pipeline message"),
], unit="reqps", description="Offset commits are real writes to a real log. On tbmq-kafka this topic carries "
                             "a sizeable fraction of a record per message that ever reaches a sensor payload."), 8, 9)
b.add(ts("Average records per Produce request", [
    prom(f'sum by (strimzi_io_cluster) (rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",topic!="",topic!="__consumer_offsets"}}[{RATE}])) / '
         f'clamp_min(sum by (strimzi_io_cluster) (rate(kafka_network_requestmetrics_requests_total{{job="kafka",request="Produce"}}[{RATE}])), 0.001)', "{{strimzi_io_cluster}}"),
], unit="suffix: records/Produce", description="How well each cluster's producers batch. Payload topics only "
                                               "(__consumer_offsets is written by the coordinator, not by a Produce request)."), 8, 9)
b.add(ts("Bytes in / out per broker", [
    prom(f'sum by (strimzi_io_cluster, pod) (rate(kafka_server_brokertopicmetrics_bytesin_total{{job="kafka",topic=""}}[{RATE}]))', "in {{strimzi_io_cluster}} {{pod}}"),
    prom(f'sum by (strimzi_io_cluster, pod) (rate(kafka_server_brokertopicmetrics_bytesout_total{{job="kafka",topic=""}}[{RATE}]))', "out {{strimzi_io_cluster}} {{pod}}"),
], unit="Bps"), 8, 9)
b.add(ts("Partitions, leaders and topics per cluster", [
    prom('sum by (strimzi_io_cluster) (kafka_server_replicamanager_partitioncount{job="kafka"})', "partitions {{strimzi_io_cluster}}"),
    prom('sum by (strimzi_io_cluster) (kafka_server_replicamanager_leadercount{job="kafka"})', "leaders {{strimzi_io_cluster}}"),
    prom('sum by (strimzi_io_cluster) (kafka_controller_kafkacontroller_globaltopiccount{job="kafka"})', "topics {{strimzi_io_cluster}}"),
    prom('sum by (strimzi_io_cluster) (kafka_controller_kafkacontroller_globalpartitioncount{job="kafka"})', "global partitions {{strimzi_io_cluster}}"),
], legend_calcs=("lastNotNull",),
    description="Every partition costs a fetch session, a log directory and a share of the broker's "
                "background work whether or not it carries traffic."), 8, 9)
b.add(ts("Messages in per topic", [
    prom(f'sum by (strimzi_io_cluster, topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{{job="kafka",topic!=""}}[{RATE}])) > 0', "{{strimzi_io_cluster}} {{topic}}"),
], unit="reqps", description="Includes __consumer_offsets, which is exactly the point."), 8, 9)

b.row("TLS (proxies only -- no metric attributes CPU to TLS)")
b.add(stat("MQTT sessions", prom('sum(connectedSessions{job="tbmq"})'), decimals=0), 4, 4)
b.add(stat("TLS sessions", prom('sum(connectedSslSessions{job="tbmq"})'), decimals=0,
           description="TBMQ counts SSL sessions separately; equal to the total means every MQTT client is on TLS."), 4, 4)
b.add(stat("Share of sessions on TLS", prom('sum(connectedSslSessions{job="tbmq"}) / clamp_min(sum(connectedSessions{job="tbmq"}), 1)'),
           unit="percentunit", decimals=1), 4, 4)
b.add(stat("MQTT bytes/s over TLS sockets", prom(f'sum(rate(container_network_receive_bytes_total{{namespace="thingsboard-mqtt-broker",pod=~"tbmq-[0-9]+"}}[{RATE}])) + sum(rate(container_network_transmit_bytes_total{{namespace="thingsboard-mqtt-broker",pod=~"tbmq-[0-9]+"}}[{RATE}]))'),
           unit="Bps", description="PROXY: all traffic on the broker pods' interfaces, which is MQTT-over-TLS plus "
                                   "plaintext Kafka, Redis, Postgres and scrape traffic. It is an upper bound on "
                                   "bytes encrypted, not a measurement of them."), 4, 4)
b.add(stat("TLS handshakes/s (proxy: connections accepted)", prom(f'sum(rate(connectionAccepted_total{{job="tbmq"}}[{RATE}]))'),
           unit="reqps", decimals=2,
           description="PROXY: every accepted MQTT connection implies one TLS handshake, the most expensive "
                       "single TLS operation. At steady state with long-lived sessions this is ~0, which is "
                       "why handshake cost does not show up in the steady-state cost-per-message numbers."), 4, 4)
b.add(stat("Connection churn/s", prom(f'sum(rate(clientDisconnects_total{{job="tbmq"}}[{RATE}])) + sum(rate(connectionError_total{{job="tbmq"}}[{RATE}]))'),
           unit="reqps", decimals=2, thresholds=ALERT_STEPS,
           description="Disconnects plus errors: churn here turns into handshake load on the panel to the left."), 4, 4)
b.add(ts("Connection and handshake events/s", [
    prom(f'sum(rate(connectionAccepted_total{{job="tbmq"}}[{RATE}]))', "accepted (= handshakes)"),
    prom(f'sum(rate(connectionRefused_total{{job="tbmq"}}[{RATE}]))', "refused"),
    prom(f'sum(rate(connectionError_total{{job="tbmq"}}[{RATE}]))', "error"),
    prom(f'sum(rate(clientDisconnects_total{{job="tbmq"}}[{RATE}]))', "disconnects"),
], unit="reqps", description="PROXY for TLS work. A reconnect storm is the one time TLS cost is visible, as a "
                             "correlated spike here and in the broker's CPU line."), 12, 9)
b.add(ts("Network bytes per pod on the message path", [
    prom(f'sum by (pod) (rate(container_network_receive_bytes_total{{namespace=~"{PIPE_NS}"}}[{RATE}]))', "rx {{pod}}"),
    prom(f'sum by (pod) (rate(container_network_transmit_bytes_total{{namespace=~"{PIPE_NS}"}}[{RATE}]))', "tx {{pod}}"),
], unit="Bps", description="Only the MQTT listener is TLS-terminated; the Kafka, Valkey and Postgres hops on "
                           "this cluster are in-cluster plaintext. Comparing the broker's line with the rest "
                           "shows how much of the total byte volume even passes through TLS."), 12, 9)

b.row("Saturation and scaling")
b.add(stat("Node CPU used", prom(f'count(count by (cpu) (node_cpu_seconds_total)) - sum(rate(node_cpu_seconds_total{{mode="idle"}}[{RATE}]))'),
           unit="none", decimals=2, description="Cores busy across the whole node, pipeline and everything else."), 4, 4)
b.add(stat("Node CPU headroom", prom(f'avg(rate(node_cpu_seconds_total{{mode="idle"}}[{RATE}]))'), unit="percentunit", decimals=1,
           thresholds=[{"color": "red", "value": None}, {"color": "orange", "value": 0.15}, {"color": "green", "value": 0.3}]), 4, 4)
b.add(stat("Node memory headroom", prom('node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes'), unit="percentunit", decimals=1,
           thresholds=[{"color": "red", "value": None}, {"color": "orange", "value": 0.15}, {"color": "green", "value": 0.3}]), 4, 4)
b.add(stat("CPU requested / allocatable", prom('sum(kube_pod_container_resource_requests{resource="cpu"}) / sum(kube_node_status_allocatable{resource="cpu"})'),
           unit="percentunit", decimals=1,
           description="What the scheduler thinks is booked. Far below actual usage means requests are set too "
                       "low for the scheduler to protect anything."), 4, 4)
b.add(stat("Pods pending", prom('sum(kube_pod_status_phase{phase="Pending"})'), decimals=0, thresholds=ALERT_STEPS), 4, 4)
b.add(stat("Restarts on the message path (1h)", prom(f'sum(increase(kube_pod_container_status_restarts_total{{namespace=~"{PIPE_NS}"}}[1h]))'),
           decimals=0, thresholds=ALERT_STEPS), 4, 4)
b.add(ts("Node CPU and memory over time", [
    prom(f'count(count by (cpu) (node_cpu_seconds_total)) - sum(rate(node_cpu_seconds_total{{mode="idle"}}[{RATE}]))', "cores busy"),
    prom(stage_cpu(PIPE_NS, ".+"), "cores: message path"),
    prom(stage_cpu("monitoring", ".+"), "cores: observability"),
    prom('count(count by (cpu) (node_cpu_seconds_total))', "cores on the node"),
], unit="none", description="If 'cores busy' approaches 'cores on the node' the node is the limit; until then "
                            "the pipeline is limited by its own concurrency, not by hardware."), 12, 9)
b.add(ts("Replicas vs their KEDA maximum", [
    prom('kube_deployment_status_replicas{namespace="iot-pipeline",deployment=~"telemetry-processor|consensus"}', "{{deployment}}"),
    prom('kube_statefulset_status_replicas{namespace="thingsboard-mqtt-broker",statefulset="tbmq-integration-executor"}', "{{statefulset}}"),
    prom('kube_horizontalpodautoscaler_spec_max_replicas{horizontalpodautoscaler=~"keda-hpa-.+"}', "max: {{horizontalpodautoscaler}}"),
    prom('kube_horizontalpodautoscaler_status_current_replicas{horizontalpodautoscaler=~"keda-hpa-.+"}', "current: {{horizontalpodautoscaler}}"),
], legend_calcs=("lastNotNull", "max"),
    description="A stage pinned at its maximum while its lag grows is the stage that needs more room."), 12, 9)
b.add(ts("KEDA scaler value vs threshold", [
    prom('sum by (scaledObject) (keda_scaler_metrics_value{job="keda-operator"})', "{{scaledObject}}"),
    prom(f'sum by (scaledObject) (rate(keda_scaler_detail_errors_total{{job="keda-operator"}}[{RATE}]))', "errors/s {{scaledObject}}"),
], description="The lag value KEDA is actually scaling on."), 12, 9)
b.add(ts("Pod restarts (1h increase) on the message path", [
    prom(f'sum by (namespace, pod) (increase(kube_pod_container_status_restarts_total{{namespace=~"{PIPE_NS}"}}[1h])) > 0', "{{namespace}}/{{pod}}"),
], legend_calcs=("lastNotNull", "max")), 12, 9)
BOARDS.append(b)

# =========================================================================================================
# Tunnel operations: the alerts, the devices, and a month of statistics per tunnel
# =========================================================================================================
# Alerts are not events in the pipeline: they are what the tunnel's own rules say about the vehicles
# that went through it (iot.speeding_alerts / iot.restricted_alerts join iot.vehicles to
# iot.tunnel_profiles and iot.tunnel_rules, both rendered from apps/simulator/config/tunnels.yaml).
# The alert panels follow the dashboard time picker (ts_entry is the instant; local_time, the column
# the tables display, is the same instant rendered in Europe/Istanbul, the boundary the rules use).
RANGE = "$__timeFilter(ts_entry)"
b = Board("iot-tunnel-operations", "Tunnel operations", ["iot", "clickhouse", "alerts"],
          "Speed-limit and restricted-vehicle alerts per tunnel over the selected range, device-level "
          "detections and health, and one month of traffic statistics (ClickHouse database iot).",
          variables=[tvar], refresh="30s", time_from="now-24h")

b.row("Alerts")
b.add(stat("Speeding vehicles", ch(f"SELECT count() AS speeding\nFROM iot.speeding_alerts\nWHERE {RANGE} AND {T}", "table"),
           thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}]), 6, 4)
b.add(stat("Restricted vehicles", ch(f"SELECT count() AS restricted\nFROM iot.restricted_alerts\nWHERE {RANGE} AND {T}", "table"),
           thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}]), 6, 4)
b.add(stat("Fastest vehicle", ch(f"SELECT max(speed_kmh) AS top_speed\nFROM iot.speeding_alerts\nWHERE {RANGE} AND {T}", "table"),
           unit="velocitykmh", decimals=1), 6, 4)
b.add(stat("Tunnels with an alert", ch(f"SELECT uniqExact(tunnel_id) AS tunnels\nFROM (\n"
                                       f"  SELECT tunnel_id FROM iot.speeding_alerts WHERE {RANGE} AND {T}\n"
                                       f"  UNION ALL\n"
                                       f"  SELECT tunnel_id FROM iot.restricted_alerts WHERE {RANGE} AND {T}\n)", "table")), 6, 4)

b.add(table("Speed limit exceeded", [
    ch(f"SELECT toString(local_time) AS time, tunnel_name AS tunnel, city, plate, vehicle_type AS type,\n"
       f"  round(speed_kmh, 1) AS speed_kmh, speed_limit_kmh AS limit_kmh, over_by_kmh, over_by_pct\n"
       f"FROM iot.speeding_alerts\nWHERE {RANGE} AND {T}\nORDER BY local_time DESC\nLIMIT 1000", "table")
], no_value="No speeding in the selected time range. Expect only a couple per tunnel per day - widen the "
            "range if the dashboard looks empty.",
   description="Which vehicle, how fast, in which tunnel, when (Europe/Istanbul) — and by how much it was over "
               "the posted limit. Counts vehicles seen by 2+ sensors and more than 5% over the limit: that "
               "margin filters out sensor speed noise on traffic cruising at the cap, leaving real speeders "
               "only. The 1000 newest alerts in the range."), 24, 9)

b.add(table("Restricted vehicle detected", [
    ch(f"SELECT toString(local_time) AS time, tunnel_name AS tunnel, city, plate, vehicle_type AS type,\n"
       f"  banned_window, round(speed_kmh, 1) AS speed_kmh, direction, lane\n"
       f"FROM iot.restricted_alerts\nWHERE {RANGE} AND {T}\nORDER BY local_time DESC\nLIMIT 1000", "table")
], no_value="No restricted vehicle entered a tunnel outside its allowed hours in the selected time range.",
   description="A vehicle type that is not allowed in that tunnel during that part of the day; times are "
               "Europe/Istanbul. The 1000 newest alerts in the range."), 24, 9)

b.add(ts("Alerts over time", [
    ch(f"SELECT $__timeInterval(ts_entry) AS time, 'speeding' AS kind, count() AS alerts\n"
       f"FROM iot.speeding_alerts\nWHERE {RANGE} AND {T}\nGROUP BY time\n"
       f"UNION ALL\n"
       f"SELECT $__timeInterval(ts_entry) AS time, 'restricted' AS kind, count() AS alerts\n"
       f"FROM iot.restricted_alerts\nWHERE {RANGE} AND {T}\nGROUP BY time\nORDER BY time")
], stack=True), 12, 8)
b.add(table("Alerts per tunnel", [
    ch(f"SELECT tunnel, sum(speeding) AS speeding, sum(restricted) AS restricted FROM (\n"
       f"  SELECT tunnel_name AS tunnel, count() AS speeding, 0 AS restricted\n"
       f"  FROM iot.speeding_alerts WHERE {RANGE} AND {T} GROUP BY tunnel\n"
       f"  UNION ALL\n"
       f"  SELECT tunnel_name AS tunnel, 0 AS speeding, count() AS restricted\n"
       f"  FROM iot.restricted_alerts WHERE {RANGE} AND {T} GROUP BY tunnel\n"
       f")\nGROUP BY tunnel\nORDER BY restricted DESC, speeding DESC", "table")
], no_value="No alerts in any tunnel in the selected time range."), 12, 8)

b.row("Devices")
b.add(ts("Detections per device", [
    prom(f'sum by (device_id, position) (rate(tunnel_sim_device_published_total[{RATE}]))', "{{device_id}} {{position}}"),
], unit="reqps", description="Live from the simulator's own /metrics (one series per device; off above "
                            "service.device_metrics_max devices)."), 12, 8)
b.add(ts("Missed detections per device", [
    prom(f'sum by (device_id, position) (rate(tunnel_sim_device_missed_total[{RATE}]))', "{{device_id}} {{position}}"),
], unit="reqps"), 12, 8)
b.add(table("Devices (latest self-report)", [
    ch(f"SELECT tunnel_id, sensor_id, device_id, device_serial AS serial, position, status,\n"
       f"  detections, missed, duplicates, round(uptime_s / 3600, 1) AS uptime_h,\n"
       f"  toString(last_report_ts) AS last_report\n"
       f"FROM iot.device_health_latest\nWHERE {T}\nORDER BY tunnel_id, position\nLIMIT 500", "table")
], description="What each device last said about itself (topic tunnels/+/sensors/+/health)."), 12, 9)
b.add(ts("Detections per device (ClickHouse, per minute)", [
    ch(f"SELECT $__timeInterval(ts) AS time, sensor_id, count() AS detections\n"
       f"FROM iot.detections\nWHERE $__timeFilter(ts) AND {T}\nGROUP BY time, sensor_id\nORDER BY time")
], description="Device-level detections straight from the stored rows, for history beyond the metrics retention."), 12, 9)

b.row("Long-term statistics (one month, per tunnel)")
b.add(ts("Vehicle types per hour", [
    ch(f"SELECT local_hour AS time, vehicle_type, sum(vehicles) AS vehicles\n"
       f"FROM iot.vehicle_types_hourly\nWHERE local_hour > now() - INTERVAL 30 DAY AND {T}\n"
       f"GROUP BY time, vehicle_type\nORDER BY time")
], stack=True), 12, 9)
b.add(ts("Vehicle types per day", [
    ch(f"SELECT toDateTime(local_day) AS time, vehicle_type, sum(vehicles) AS vehicles\n"
       f"FROM iot.vehicle_types_daily\nWHERE local_day > toDate(now(), 'Europe/Istanbul') - 30 AND {T}\n"
       f"GROUP BY time, vehicle_type\nORDER BY time")
], stack=True), 12, 9)
b.add(ts("Average speed per hour", [
    ch(f"SELECT local_hour AS time, tunnel_id, sum(vehicles * speed_avg_kmh) / greatest(sum(vehicles), 1) AS speed\n"
       f"FROM iot.vehicle_types_hourly\nWHERE local_hour > now() - INTERVAL 30 DAY AND {T}\n"
       f"GROUP BY time, tunnel_id\nORDER BY time")
], unit="velocitykmh", min0=False), 12, 9)
b.add(ts("Average speed per day", [
    ch(f"SELECT toDateTime(local_day) AS time, tunnel_id, sum(vehicles * speed_avg_kmh) / greatest(sum(vehicles), 1) AS speed\n"
       f"FROM iot.vehicle_types_daily\nWHERE local_day > toDate(now(), 'Europe/Istanbul') - 30 AND {T}\n"
       f"GROUP BY time, tunnel_id\nORDER BY time")
], unit="velocitykmh", min0=False), 12, 9)
b.add(table("Tunnel profiles", [
    ch("SELECT tunnel_id, name, city, city_code, round(length_m) AS length_m, lanes_per_direction AS lanes,\n"
       "  speed_limit_kmh AS limit_kmh\nFROM iot.tunnel_profiles FINAL\nORDER BY tunnel_id", "table")
], description="The configured tunnels (apps/simulator/config/tunnels.yaml)."), 12, 8)
b.add(table("Access rules", [
    ch("SELECT tunnel_id, vehicle_type,\n"
       "  concat(leftPad(toString(intDiv(from_minute, 60)), 2, '0'), ':', leftPad(toString(from_minute % 60), 2, '0'),\n"
       "         '-', leftPad(toString(intDiv(to_minute, 60)), 2, '0'), ':', leftPad(toString(to_minute % 60), 2, '0'))\n"
       "    AS banned_window\nFROM iot.tunnel_rules FINAL\nORDER BY tunnel_id, vehicle_type", "table")
], description="When each vehicle type may not use a tunnel (local time)."), 12, 8)
BOARDS.append(b)

OUT.mkdir(parents=True, exist_ok=True)
names = {"iot-pipeline-overview": "iot-pipeline-overview.json", "iot-kafka": "kafka.json", "iot-tbmq": "tbmq.json",
         "iot-tunnel-traffic": "tunnel-traffic.json", "iot-tunnel-operations": "tunnel-operations.json",
         "iot-pipeline-economics": "pipeline-economics.json"}
for board in BOARDS:
    path = OUT / names[board.uid]
    path.write_text(json.dumps(board.json(), indent=2) + "\n")
    print(path, len(board.panels), "panels")
