# ClickHouse: single replica, no Keeper. Hot data on the PVC, cold parts on SeaweedFS S3 (storage
# policy "tiered"), Kafka engine ingestion from the app Kafka (config.d/40-kafka.xml, schema/).
#
# Server config: ConfigMaps clickhouse-config (config.d) and clickhouse-users-config (users.d), generated
# by kustomize with a hash suffix → a config change rolls the pod. Secrets (Reloader restarts on
# change): clickhouse-users (passwords), clickhouse-kafka (KafkaUser password + cluster CA, copied
# from ${PIPELINE_NAMESPACE}), seaweedfs-s3 (S3 keys).
apiVersion: v1
kind: Service
metadata:
  name: clickhouse
  namespace: ${STORAGE_NAMESPACE}
  labels:
    app: clickhouse
spec:
  type: ClusterIP
  selector:
    app: clickhouse
  ports:
    - name: http
      port: 8123
      targetPort: http
    - name: native
      port: 9000
      targetPort: native
    - name: metrics
      port: 9363
      targetPort: metrics
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: clickhouse
  namespace: ${STORAGE_NAMESPACE}
  labels:
    app: clickhouse
  annotations:
    reloader.stakater.com/auto: "true"   # renewed Kafka CA / rotated passwords
spec:
  serviceName: clickhouse
  replicas: 1
  selector:
    matchLabels:
      app: clickhouse
  template:
    metadata:
      labels:
        app: clickhouse                    # app Kafka NetworkPolicy admits this label from ${STORAGE_NAMESPACE}
        app.kubernetes.io/part-of: iot-infrastructure
    spec:
      # Kafka consumers leave their groups and in-flight blocks finish or are discarded uncommitted.
      terminationGracePeriodSeconds: 60
      securityContext:
        runAsNonRoot: true
        runAsUser: 101                     # image user "clickhouse"
        runAsGroup: 101
        fsGroup: 101
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: clickhouse
          image: ${CLICKHOUSE_IMAGE}
          imagePullPolicy: IfNotPresent
          # Not the image entrypoint (root chown, generated users.d). umask 027: preprocessed configs
          # on the PVC (they contain the Kafka/S3 secrets) are not world-readable on the node.
          command: ["/bin/sh", "-c", "umask 027 && exec /usr/bin/clickhouse-server --config-file=/etc/clickhouse-server/config.xml"]
          env:
            - name: CLICKHOUSE_WATCHDOG_ENABLE   # server is PID 1 and gets SIGTERM directly
              value: "0"
            - name: CLICKHOUSE_PASSWORD          # clickhouse-client in the pod (user default, localhost)
              valueFrom:
                secretKeyRef:
                  name: clickhouse-users
                  key: default-password
            - name: CLICKHOUSE_DEFAULT_PASSWORD_SHA256
              valueFrom:
                secretKeyRef:
                  name: clickhouse-users
                  key: default-password-sha256
            - name: CLICKHOUSE_GRAFANA_PASSWORD_SHA256
              valueFrom:
                secretKeyRef:
                  name: clickhouse-users
                  key: grafana-password-sha256
            - name: APP_KAFKA_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: clickhouse-kafka
                  key: password
            - name: S3_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: seaweedfs-s3
                  key: access-key
            - name: S3_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: seaweedfs-s3
                  key: secret-key
          ports:
            - name: http
              containerPort: 8123
            - name: native
              containerPort: 9000
            - name: metrics
              containerPort: 9363
          startupProbe:                        # loading many parts after a crash can take a while
            httpGet:
              path: /ping
              port: http
            periodSeconds: 5
            failureThreshold: 120
          readinessProbe:
            httpGet:
              path: /ping
              port: http
            periodSeconds: 10
            timeoutSeconds: 3
          livenessProbe:
            httpGet:
              path: /ping
              port: http
            periodSeconds: 20
            timeoutSeconds: 5
            failureThreshold: 6
          resources:
            requests:
              cpu: ${CLICKHOUSE_CPU_REQUEST}
              memory: ${CLICKHOUSE_MEMORY_REQUEST}
            limits:
              memory: ${CLICKHOUSE_MEMORY_LIMIT}  # server caps itself at 80% (config.d/10-server.xml)
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: data
              mountPath: /var/lib/clickhouse     # hot parts, S3 part metadata, S3 read cache
            - name: config
              mountPath: /etc/clickhouse-server/config.d
              readOnly: true
            - name: users
              mountPath: /etc/clickhouse-server/users.d
              readOnly: true
            - name: app-kafka-ca
              mountPath: /etc/app-kafka
              readOnly: true
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: config
          configMap:
            name: clickhouse-config
        - name: users
          configMap:
            name: clickhouse-users-config
        - name: app-kafka-ca
          secret:
            secretName: clickhouse-kafka
            items:
              - key: ca.crt
                path: ca.crt
        - name: tmp
          emptyDir:
            sizeLimit: 64Mi
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: local-path
        resources:
          requests:
            storage: ${CLICKHOUSE_STORAGE}
