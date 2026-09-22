# KEDA's SCRAM login to the app Kafka (KafkaUser keda: Describe on iot.* topics and the consumer
# groups). Secret keda-app-kafka is synced from the KafkaUser secret by scripts/install/80-processing.sh.
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: app-kafka-keda
  namespace: iot-pipeline
spec:
  secretTargetRef:
    - parameter: sasl
      name: keda-app-kafka
      key: sasl
    - parameter: username
      name: keda-app-kafka
      key: username
    - parameter: password
      name: keda-app-kafka
      key: password
    - parameter: tls
      name: keda-app-kafka
      key: tls
    - parameter: ca
      name: keda-app-kafka
      key: ca
