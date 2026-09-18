# Rendered to .generated/tbmq-deps/ by scripts/install/60-tbmq-deps.sh.
# TBMQ data services, operator-managed: Strimzi Kafka, CloudNativePG Postgres, plus a
# Valkey StatefulSet. Namespace and TBMQ config maps come from the upstream manifests.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - ../../${TBMQ_MANIFESTS_DIR}/tbmq-namespace.yml
  - ../../${TBMQ_MANIFESTS_DIR}/tbmq-configmap.yml
  - ../../${TBMQ_MANIFESTS_DIR}/tbmq-ie-configmap.yml
  - kafka.yaml                        # rendered from deploy/tbmq-deps/kafka.yaml.tpl
  - postgres.yaml                     # rendered from deploy/tbmq-deps/postgres.yaml.tpl
  - valkey.yaml                       # rendered from deploy/tbmq-deps/valkey.yaml.tpl

labels:
  - pairs:
      app.kubernetes.io/part-of: iot-infrastructure
    includeSelectors: false

patches:
  # Datasource → CNPG read-write service. The credentials come from the CNPG app secret
  # (env on the pods, deploy/tbmq/data-clients), not from this config map.
  - target:
      kind: ConfigMap
      name: tbmq-db-config
    patch: |-
      - op: replace
        path: /data/SPRING_DATASOURCE_URL
        value: jdbc:postgresql://tbmq-db-rw:5432/thingsboard_mqtt_broker
      - op: remove
        path: /data/SPRING_DATASOURCE_USERNAME
      - op: remove
        path: /data/SPRING_DATASOURCE_PASSWORD
