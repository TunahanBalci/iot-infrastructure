apiVersion: v1
kind: Namespace
metadata:
  name: ${STORAGE_NAMESPACE}
  labels:
    app.kubernetes.io/part-of: iot-infrastructure
