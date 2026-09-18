# TBMQ metadata database (CloudNativePG): credentials, ACLs, users, settings.
# Services: tbmq-db-rw (primary), tbmq-db-ro / tbmq-db-r (replicas).
# App credentials: secret tbmq-db-app (username, password, jdbc-uri, ...), owner of the database.
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: tbmq-db
  namespace: thingsboard-mqtt-broker
spec:
  instances: ${POSTGRES_INSTANCES}
  imageName: ${POSTGRES_IMAGE}
  primaryUpdateStrategy: unsupervised
  enableSuperuserAccess: false
  bootstrap:
    initdb:
      database: thingsboard_mqtt_broker
      owner: tbmq
  storage:
    size: ${POSTGRES_STORAGE}
    storageClass: local-path
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      memory: 1Gi
  affinity:
    enablePodAntiAffinity: true
    podAntiAffinityType: preferred    # single node: replicas may share it
