# consensus: fuses detections (iot.detections, keyed by tunnel) into vehicle events and per-tunnel
# traffic reports (iot.vehicles, iot.traffic). Device health is NOT produced here — the devices
# report their own condition and telemetry-processor routes it to iot.sensor-health.
# Stateful per tunnel: a pod owns the tunnels of
# its Kafka partitions, commits only past fused detections and keeps learned geometry in the compacted
# iot.consensus.geometry topic, so KEDA scale-out/in only rebalances. Scale-in is deliberately slow.
# The ScaledObject lives in consensus-scaler.yaml.tpl: it needs VictoriaMetrics (see there).
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consensus
  namespace: iot-pipeline
  labels:
    app: consensus
  annotations:
    reloader.stakater.com/auto: "true"
spec:
  # replicas: owned by KEDA (ScaledObject below)
  selector:
    matchLabels:
      app: consensus
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    metadata:
      labels:
        app: consensus
    spec:
      terminationGracePeriodSeconds: 60   # commit watermarks, save geometry, leave the group
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
      containers:
        - name: consensus
          image: ${CONSENSUS_IMAGE_REF}
          imagePullPolicy: Never          # imported into k3s containerd by the install step
          ports:
            - name: http
              containerPort: 8080
          env:
            - name: CONSENSUS__INPUT__SOURCE
              value: kafka
            - name: CONSENSUS__OUTPUT__SINK
              value: kafka
            - name: CONSENSUS__KAFKA__BOOTSTRAP_SERVERS
              value: app-kafka-kafka-bootstrap.iot-pipeline.svc:9093
            # Records per consume() call: bigger batches amortise the per-call overhead and let the
            # producer fill larger Kafka batches, at the cost of a longer commit cycle.
            - name: CONSENSUS__KAFKA__POLL_BATCH
              value: "${CONSENSUS_BATCH_SIZE}"
            - name: CONSENSUS__KAFKA__SECURITY_PROTOCOL
              value: SASL_SSL
            - name: CONSENSUS__KAFKA__SASL_MECHANISM
              value: SCRAM-SHA-512
            - name: CONSENSUS__KAFKA__SASL_USERNAME
              value: consensus
            - name: CONSENSUS__KAFKA__SASL_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: consensus
                  key: password
            - name: CONSENSUS__KAFKA__SSL_CA_LOCATION
              value: /etc/app-kafka/ca.crt
            - name: CONSENSUS__REPORTING__INTERVAL_S
              value: "${CONSENSUS_TRAFFIC_INTERVAL_S}"
            - name: CONSENSUS__ASSOCIATION__DEDUP_WINDOW
              value: "128"                # same results as 512, ~70 MiB less at 1000 tunnels
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: "${OTEL_ENDPOINT}"
            - name: OTEL_SERVICE_NAME
              value: consensus
            - name: OTEL_TRACES_SAMPLER
              value: parentbased_traceidratio
            - name: OTEL_TRACES_SAMPLER_ARG
              value: "${TRACES_SAMPLE_RATIO}"
          readinessProbe:                 # group joined and geometry loaded
            httpGet:
              path: /readyz
              port: http
            periodSeconds: 5
            failureThreshold: 3
          livenessProbe:                  # 503 when the main loop stalls for 120 s
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 6
          resources:
            requests:
              cpu: ${CONSENSUS_CPU_REQUEST}
              memory: ${CONSENSUS_MEMORY_REQUEST}
            limits:
              memory: ${CONSENSUS_MEMORY_LIMIT}
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
