# SeaweedFS: one pod running master + volume server + filer + S3 gateway (weed server -s3) on a
# PVC. S3 cold tier for ClickHouse (bucket clickhouse-cold, created by the install step).
#
# Only the S3 API (8333, signed requests: identities from Secret seaweedfs-s3) and the metrics port
# (9327) listen on the pod IP. Master, volume server and filer have no authentication and bind to
# 127.0.0.1 (admin: kubectl -n ${STORAGE_NAMESPACE} port-forward seaweedfs-0 9333 8888).
apiVersion: v1
kind: Service
metadata:
  name: seaweedfs
  namespace: ${STORAGE_NAMESPACE}
  labels:
    app: seaweedfs
spec:
  type: ClusterIP
  selector:
    app: seaweedfs
  ports:
    - name: s3
      port: 8333
      targetPort: s3
    - name: metrics
      port: 9327
      targetPort: metrics
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: seaweedfs
  namespace: ${STORAGE_NAMESPACE}
  labels:
    app: seaweedfs
  annotations:
    reloader.stakater.com/auto: "true"   # restart on new S3 identities (Secret seaweedfs-s3)
spec:
  serviceName: seaweedfs
  replicas: 1
  selector:
    matchLabels:
      app: seaweedfs
  template:
    metadata:
      labels:
        app: seaweedfs
        app.kubernetes.io/part-of: iot-infrastructure
    spec:
      terminationGracePeriodSeconds: 30
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000                    # image user "seaweed"
        runAsGroup: 1000
        fsGroup: 1000
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: seaweedfs
          image: ${SEAWEEDFS_IMAGE}
          imagePullPolicy: IfNotPresent
          # Not the image entrypoint: it starts as root to chown /data.
          command: ["/usr/bin/weed"]
          args:
            - -logtostderr=true
            - server
            - -dir=/data
            - -ip=127.0.0.1
            - -ip.bind=127.0.0.1
            - -master.volumeSizeLimitMB=1024
            - -master.defaultReplication=000
            - -volume.max=0                  # volumes = free disk / 1 GiB
            - -volume.index=leveldb          # needle index on disk, not in memory
            - -filer
            - -s3
            - -s3.ip.bind=0.0.0.0
            - -s3.port=8333
            - -s3.config=/etc/seaweedfs/s3/s3.json
            - -s3.iam=false                  # identities only from s3.json
            - -s3.port.iceberg=0             # no Iceberg REST catalog (default 0.0.0.0:8181)
            - -s3.port.lance=0
            - -metricsIp=0.0.0.0
            - -metricsPort=9327
          env:
            - name: GOMEMLIMIT                 # Go GC soft limit below the container limit
              value: "${SEAWEEDFS_GOMEMLIMIT}"
          ports:
            - name: s3
              containerPort: 8333
            - name: metrics
              containerPort: 9327
          startupProbe:                        # S3 starts after master, volume and filer (~15 s)
            httpGet:
              path: /healthz
              port: s3
            periodSeconds: 2
            failureThreshold: 90
          readinessProbe:
            httpGet:
              path: /healthz
              port: s3
            periodSeconds: 10
            timeoutSeconds: 3
          livenessProbe:
            httpGet:
              path: /healthz
              port: s3
            periodSeconds: 20
            timeoutSeconds: 5
            failureThreshold: 6
          resources:
            requests:
              cpu: 50m
              memory: ${SEAWEEDFS_MEMORY_REQUEST}
            limits:
              memory: ${SEAWEEDFS_MEMORY_LIMIT}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: data
              mountPath: /data                 # volumes, master state, filer leveldb (/data/filerldb2)
            - name: s3-config
              mountPath: /etc/seaweedfs/s3
              readOnly: true
            - name: tmp
              mountPath: /tmp                  # gRPC unix sockets
      volumes:
        - name: s3-config
          secret:
            secretName: seaweedfs-s3
            items:
              - key: s3.json
                path: s3.json
        - name: tmp
          emptyDir:
            sizeLimit: 16Mi
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: local-path
        resources:
          requests:
            storage: ${SEAWEEDFS_STORAGE}
