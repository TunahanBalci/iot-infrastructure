# Rendered to .generated/tbmq/kustomization.yaml by scripts/install/70-tbmq.sh.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - ../../${TBMQ_MANIFESTS_DIR}/tbmq.yml
  - ../../${TBMQ_MANIFESTS_DIR}/tbmq-ie.yml
  - edge.yaml                         # rendered from deploy/tbmq/edge.yaml.tpl

components:
  - ../../deploy/tbmq/data-clients    # Strimzi Kafka, CNPG Postgres, Valkey password

labels:
  - pairs:
      app.kubernetes.io/part-of: iot-infrastructure
    includeSelectors: false

replicas:
  - name: tbmq
    count: ${TBMQ_REPLICAS}
  - name: tbmq-integration-executor
    count: ${TBMQ_IE_REPLICAS}

# Device CA certificate (public part only), written by 70-tbmq.sh. The hash suffix rolls the
# broker when the CA changes.
configMapGenerator:
  - name: tbmq-device-ca
    namespace: thingsboard-mqtt-broker
    files:
      - ca.crt=device-ca.crt

patches:
  - path: ../../deploy/tbmq/patches/tbmq-statefulset.yaml
  # Upstream NodePort Service → ClusterIP. Kept (same name) because it is the
  # StatefulSet's governing service; Envoy uses it for the UI on 8083.
  - path: ../../deploy/tbmq/patches/tbmq-service.yaml
    target:
      kind: Service
      name: tbmq
  - target:
      kind: StatefulSet
    patch: |-
      - op: replace
        path: /spec/template/spec/containers/0/imagePullPolicy
        value: IfNotPresent
  # Rolling restart when a referenced Secret/ConfigMap changes (Reloader): renewed MQTTS keystore
  # (cert-manager), Kafka client certificate and cluster CA (Strimzi).
  - target:
      kind: StatefulSet
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: all
        annotations:
          reloader.stakater.com/auto: "true"
  # Lean JVMs with memory limits (upstream sets neither: the JVM would size its heap from host RAM).
  - target:
      kind: StatefulSet
      name: tbmq
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: tbmq
      spec:
        template:
          spec:
            containers:
              - name: server
                env:
                  - name: JAVA_OPTS
                    value: "-Xms${TBMQ_HEAP} -Xmx${TBMQ_HEAP}"
                  # Consumer of tbmq.msg.all (the hot path): see TBMQ_MSG_CONSUMER_CONFIG in config.env.
                  - name: TB_KAFKA_MSG_ALL_ADDITIONAL_CONSUMER_CONFIG
                    value: "${TBMQ_MSG_CONSUMER_CONFIG}"
                resources:
                  requests:
                    cpu: 250m
                    memory: ${TBMQ_MEMORY_LIMIT}
                  limits:
                    memory: ${TBMQ_MEMORY_LIMIT}
  - target:
      kind: StatefulSet
      name: tbmq-integration-executor
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: tbmq-integration-executor
      spec:
        template:
          spec:
            containers:
              - name: server
                env:
                  - name: JAVA_OPTS
                    value: "-Xms${TBMQ_IE_HEAP} -Xmx${TBMQ_IE_HEAP}"
                  # Consumer of the tbmq.msg.ie.* topics: see TBMQ_IE_MSG_CONSUMER_CONFIG in config.env.
                  - name: TB_KAFKA_IE_MSG_ADDITIONAL_CONSUMER_CONFIG
                    value: "${TBMQ_IE_MSG_CONSUMER_CONFIG}"
                resources:
                  requests:
                    cpu: 250m
                    memory: ${TBMQ_IE_MEMORY_REQUEST}
                  limits:
                    memory: ${TBMQ_IE_MEMORY_LIMIT}
  # Upstream requires one integration executor per node; KEDA may run more on this single node.
  - target:
      kind: StatefulSet
      name: tbmq-integration-executor
    patch: |-
      - op: replace
        path: /spec/template/spec/affinity
        value:
          podAntiAffinity:
            preferredDuringSchedulingIgnoredDuringExecution:
              - weight: 100
                podAffinityTerm:
                  topologyKey: kubernetes.io/hostname
                  labelSelector:
                    matchLabels:
                      app: tbmq-integration-executor
  # TBMQ topic replication factor (generated; no-op while KAFKA_REPLICAS=1).
  - path: kafka-topics-tbmq.yaml
  - path: kafka-topics-ie.yaml

replacements:
  - source:
      kind: StatefulSet
      name: tbmq
      fieldPath: spec.template.spec.containers.[name=server].image
    targets:
      - select:
          kind: StatefulSet
          name: tbmq
        fieldPaths:
          - spec.template.spec.initContainers.[name=tls-keystore].image
