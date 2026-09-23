# Rendered to .generated/storage/kustomization.yaml by scripts/install/85-storage.sh.
# Storage layer: ClickHouse (hot/warm, Kafka ingestion) + SeaweedFS (S3 cold tier).
# Secrets (seaweedfs-s3, clickhouse-users, clickhouse-grafana, clickhouse-kafka) are created by the
# install step from .state/ and the pipeline namespace; they are never rendered to disk.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - namespace.yaml                    # rendered from deploy/storage/namespace.yaml.tpl
  - seaweedfs.yaml                    # rendered from deploy/storage/seaweedfs.yaml.tpl
  - clickhouse.yaml                   # rendered from deploy/storage/clickhouse.yaml.tpl
  - networkpolicy.yaml                # rendered from deploy/storage/networkpolicy.yaml.tpl

labels:
  - pairs:
      app.kubernetes.io/part-of: iot-infrastructure
    includeSelectors: false

# Server config and users (rendered XML). The hash suffix rolls ClickHouse when they change.
configMapGenerator:
  - name: clickhouse-config
    namespace: ${STORAGE_NAMESPACE}
    files:
      - clickhouse/config.d/10-server.xml
      - clickhouse/config.d/20-system-logs.xml
      - clickhouse/config.d/30-storage.xml
      - clickhouse/config.d/40-kafka.xml
  - name: clickhouse-users-config
    namespace: ${STORAGE_NAMESPACE}
    files:
      - clickhouse/users.d/users.xml
