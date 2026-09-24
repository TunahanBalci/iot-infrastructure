# The tunnel simulator runs as a Docker container on the host network (make sim-up), outside the cluster,
# and serves tunnel_sim_* metrics on ${K3S_NODE_IP}:${SIM_METRICS_PORT} (the node's host-local dummy
# interface, reachable from pods). Rendered and applied by scripts/install/90-observability.sh; the
# address changes with K3S_NODE_IP / SIM_METRICS_PORT, so it is a template, not part of scrapes.yaml.
#
# The simulator is usually not running: its target is deliberately left out of the IoTTargetDown and
# IoTMetricsMissing alerts, and `tunnel_sim_up` (see the alerts in alerts.yaml) is only used while it runs.
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMStaticScrape
metadata:
  name: simulator
  namespace: monitoring
spec:
  jobName: simulator
  targetEndpoints:
    - targets: ["${K3S_NODE_IP}:${SIM_METRICS_PORT}"]
      path: /metrics
      scrape_interval: 10s
      scrapeTimeout: 5s
      labels:
        namespace: host
      relabelConfigs:
        - targetLabel: job
          replacement: simulator
