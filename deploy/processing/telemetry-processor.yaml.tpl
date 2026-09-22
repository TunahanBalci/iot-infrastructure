# telemetry-processor: validates TBMQ integration envelopes (iot.mqtt.ingest), checks the MQTT topic
# against the client certificate CN, re-keys by tunnel id → iot.detections (rejects → iot.detections.rejected).
# Stateless; KEDA scales it on the lag of consumer group telemetry-processor.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: telemetry-processor
  namespace: iot-pipeline
  labels:
    app: telemetry-processor
  annotations:
    reloader.stakater.com/auto: "true"  # app Kafka CA / SCRAM password changes
spec:
  # replicas: owned by KEDA (ScaledObject below)
  selector:
    matchLabels:
      app: telemetry-processor
  template:
    metadata:
      labels:
        app: telemetry-processor
    spec:
      terminationGracePeriodSeconds: 30   # flush + commit before the group rebalances
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
      containers:
        - name: telemetry-processor
          image: ${PROCESSOR_IMAGE_REF}
          imagePullPolicy: Never          # imported into k3s containerd by the install step
          ports:
            - name: http
              containerPort: 8080
          env:
            - name: PROCESSOR__KAFKA__BOOTSTRAP_SERVERS
              value: app-kafka-kafka-bootstrap.iot-pipeline.svc:9093
            # Records per consume() call: bigger batches amortise the per-call overhead and let the
            # producer fill larger Kafka batches, at the cost of a longer commit cycle.
            - name: PROCESSOR__KAFKA__POLL_BATCH
              value: "${PROCESSOR_BATCH_SIZE}"
            - name: PROCESSOR__KAFKA__SECURITY_PROTOCOL
              value: SASL_SSL
            - name: PROCESSOR__KAFKA__SASL_MECHANISM
              value: SCRAM-SHA-512
            - name: PROCESSOR__KAFKA__SASL_USERNAME
              value: telemetry-processor
            - name: PROCESSOR__KAFKA__SASL_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: telemetry-processor
                  key: password
            - name: PROCESSOR__KAFKA__SSL_CA_LOCATION
              value: /etc/app-kafka/ca.crt
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: "${OTEL_ENDPOINT}"
            - name: OTEL_SERVICE_NAME
              value: telemetry-processor
            - name: OTEL_TRACES_SAMPLER
              value: parentbased_traceidratio
            - name: OTEL_TRACES_SAMPLER_ARG
              value: "${TRACES_SAMPLE_RATIO}"
          readinessProbe:
            httpGet:
              path: /readyz
              port: http
            periodSeconds: 5
            failureThreshold: 3
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 10
            periodSeconds: 10
            failureThreshold: 3
          resources:
            requests:
              cpu: ${PROCESSOR_CPU_REQUEST}
              memory: ${PROCESSOR_MEMORY_REQUEST}
            limits:
              memory: ${PROCESSOR_MEMORY_LIMIT}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: app-kafka-ca
              mountPath: /etc/app-kafka
              readOnly: true
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: app-kafka-ca
          secret:
            secretName: app-kafka-cluster-ca-cert
            items:
              - key: ca.crt
                path: ca.crt
        - name: tmp
          emptyDir:
            sizeLimit: 16Mi
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: telemetry-processor
  namespace: iot-pipeline
spec:
  scaleTargetRef:
    name: telemetry-processor
  minReplicaCount: ${PROCESSOR_MIN_REPLICAS}
  maxReplicaCount: ${PROCESSOR_MAX_REPLICAS}
  pollingInterval: 15
  cooldownPeriod: 120
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 120
  triggers:
    - type: kafka
      authenticationRef:
        name: app-kafka-keda
      metadata:
        bootstrapServers: app-kafka-kafka-bootstrap.iot-pipeline.svc:9093
        consumerGroup: telemetry-processor
        topic: iot.mqtt.ingest
        lagThreshold: "${PROCESSOR_LAG_THRESHOLD}"
        offsetResetPolicy: earliest
        allowIdleConsumers: "false"
