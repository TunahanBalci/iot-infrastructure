# Rendered to .generated/tbmq-db-setup/ by scripts/install/60-tbmq-deps.sh.
# Upstream one-shot schema install pod, pointed at the CNPG database and Valkey.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - ../../${TBMQ_MANIFESTS_DIR}/database-setup.yml

components:
  - ../../deploy/tbmq/data-clients
