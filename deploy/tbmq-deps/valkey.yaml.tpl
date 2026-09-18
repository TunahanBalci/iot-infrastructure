# TBMQ session state and cache (Valkey, standalone). AOF persistence on a PVC and a
# password (secret tbmq-valkey, created by scripts/install/60-tbmq-deps.sh).
apiVersion: v1
kind: Service
metadata:
  name: tbmq-valkey
  namespace: thingsboard-mqtt-broker
spec:
  type: ClusterIP
  selector:
    app: tbmq-valkey
  ports:
    - name: valkey
      port: 6379
      targetPort: valkey
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: tbmq-valkey
  namespace: thingsboard-mqtt-broker
spec:
  serviceName: tbmq-valkey
  replicas: 1
  selector:
    matchLabels:
      app: tbmq-valkey
  template:
    metadata:
      labels:
        app: tbmq-valkey
    spec:
      terminationGracePeriodSeconds: 30   # final AOF fsync on SIGTERM
      containers:
        - name: valkey
          image: ${VALKEY_IMAGE}
          imagePullPolicy: IfNotPresent
          # Image entrypoint drops to the valkey user and owns /data.
          args:
            - valkey-server
            - --dir
            - /data
            - --appendonly
            - "yes"
            - --appendfsync
            - everysec
            - --requirepass
            - $(VALKEY_PASSWORD)
            - --maxmemory                 # below the container limit: refuse writes instead of an OOM kill
            - ${VALKEY_MAXMEMORY}
            - --maxmemory-policy
            - noeviction                  # TBMQ session state must not be evicted silently
          env:
            - name: VALKEY_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: tbmq-valkey
                  key: password
          ports:
            - name: valkey
              containerPort: 6379
          readinessProbe:                 # not ready while the AOF is still loading
            exec:
              command: ["sh", "-c", 'valkey-cli --no-auth-warning -a "$VALKEY_PASSWORD" ping | grep -q PONG']
            periodSeconds: 5
            timeoutSeconds: 3
          livenessProbe:
            tcpSocket:
              port: valkey
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            requests:
              cpu: 50m
              memory: 128Mi
            limits:
              memory: 1Gi
          volumeMounts:
            - name: data
              mountPath: /data
        - name: metrics                   # redis_exporter for Valkey (memory, clients, AOF), :9121
          image: ${VALKEY_EXPORTER_IMAGE}
          imagePullPolicy: IfNotPresent
          env:
            - name: REDIS_ADDR
              value: redis://127.0.0.1:6379
            - name: REDIS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: tbmq-valkey
                  key: password
          ports:
            - name: metrics
              containerPort: 9121
          readinessProbe:
            httpGet:
              path: /health
              port: metrics
            periodSeconds: 10
          securityContext:
            runAsNonRoot: true
            runAsUser: 59000
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
            limits:
              memory: 48Mi
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: local-path
        resources:
          requests:
            storage: ${VALKEY_STORAGE}
