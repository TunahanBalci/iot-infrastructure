# Local PKI: self-signed root → CA issuer used for every cluster certificate.
# Replace iot-ca with an ACME ClusterIssuer when the edge has a public domain.
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned-bootstrap
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: iot-root-ca
  namespace: cert-manager
spec:
  isCA: true
  commonName: ${DOMAIN} root CA
  secretName: iot-root-ca
  duration: 87600h
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef:
    kind: ClusterIssuer
    name: selfsigned-bootstrap
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: iot-ca
spec:
  ca:
    secretName: iot-root-ca
---
# Device PKI: separate self-signed root for MQTT client certificates (mTLS on 8883).
# TBMQ trusts only this CA for clients and maps the certificate CN to ACL rules
# (deploy/tbmq/device-credentials.json.tpl). Bulk device certs: scripts/device-certs.sh.
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: iot-device-ca
  namespace: cert-manager
spec:
  isCA: true
  commonName: ${DOMAIN} device CA
  secretName: iot-device-ca
  duration: 87600h
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef:
    kind: ClusterIssuer
    name: selfsigned-bootstrap
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: iot-device-ca
spec:
  ca:
    secretName: iot-device-ca
