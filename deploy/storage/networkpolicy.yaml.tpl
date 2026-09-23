# Ingress rules for the storage namespace (enforced by Cilium). Egress stays open: ClickHouse
# reaches the app Kafka (whose own NetworkPolicy admits app=clickhouse from ${STORAGE_NAMESPACE}).
# kubectl exec / port-forward are not affected.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: clickhouse
  namespace: ${STORAGE_NAMESPACE}
spec:
  podSelector:
    matchLabels:
      app: clickhouse
  policyTypes: ["Ingress"]
  ingress:
    # Grafana (SQL over HTTP/native) and the metrics scraper (9363) in the monitoring namespace.
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ${MONITORING_NAMESPACE}
      ports:
        - port: http
        - port: native
        - port: metrics
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: seaweedfs
  namespace: ${STORAGE_NAMESPACE}
spec:
  podSelector:
    matchLabels:
      app: seaweedfs
  policyTypes: ["Ingress"]
  ingress:
    # S3 only from ClickHouse.
    - from:
        - podSelector:
            matchLabels:
              app: clickhouse
      ports:
        - port: s3
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ${MONITORING_NAMESPACE}
      ports:
        - port: metrics
