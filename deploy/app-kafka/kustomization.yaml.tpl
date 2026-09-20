# Rendered to .generated/app-kafka/ by scripts/install/65-app-kafka.sh.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - kafka.yaml                        # rendered from deploy/app-kafka/kafka.yaml.tpl
  - topics.yaml                       # rendered from deploy/app-kafka/topics.yaml.tpl
  - ../../deploy/app-kafka/users.yaml

labels:
  - pairs:
      app.kubernetes.io/part-of: iot-infrastructure
    includeSelectors: false
