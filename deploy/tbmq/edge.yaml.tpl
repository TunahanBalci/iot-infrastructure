# TBMQ exposure. Division of work:
#   Cilium L4 (tbmq-mqtt, ${MQTT_LB_IP}): MQTTS 8883 only, client certificate required — no proxy in path
#   Envoy L7 (Gateway edge, ${EDGE_LB_IP}): UI/REST, UI live updates
# Plain MQTT 1883 is in-cluster only (service tbmq). MQTT over WebSocket is not exposed: Envoy
# terminates TLS, so device certificates could not reach TBMQ.

# --- TLS for MQTTS (terminated by TBMQ) --------------------------------------
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: tbmq-mqtt-tls
  namespace: thingsboard-mqtt-broker
spec:
  secretName: tbmq-mqtt-tls
  commonName: mqtt.${DOMAIN}
  dnsNames:
    - mqtt.${DOMAIN}
    - tbmq.thingsboard-mqtt-broker.svc
    - tbmq-mqtt.thingsboard-mqtt-broker.svc
  ipAddresses:
    - ${MQTT_LB_IP}
  privateKey:
    algorithm: RSA
    size: 2048
    encoding: PKCS8
    rotationPolicy: Always
  # TBMQ reads the listener credentials from a PKCS#12 keystore: its init container adds the
  # device CA as a trusted entry (a PEM file can't carry extra trust anchors). Password: secret
  # tbmq-mqtt-keystore, created by scripts/install/70-tbmq.sh.
  keystores:
    pkcs12:
      create: true
      profile: Modern2023
      passwordSecretRef:
        name: tbmq-mqtt-keystore
        key: password
  issuerRef:
    kind: ClusterIssuer
    name: iot-ca
---
# --- L4: MQTTS through Cilium eBPF (Maglev) -----------------------------------
apiVersion: v1
kind: Service
metadata:
  name: tbmq-mqtt
  namespace: thingsboard-mqtt-broker
  annotations:
    lbipam.cilium.io/ips: "${MQTT_LB_IP}"
spec:
  type: LoadBalancer
  externalTrafficPolicy: Cluster
  # Cilium serves the LB IP directly; NodePorts would expose MQTTS on the host's Wi-Fi IP.
  allocateLoadBalancerNodePorts: false
  # No sessionAffinity: clients behind one NAT/IP would all land on one pod.
  selector:
    app: tbmq
  ports:
    - name: mqtt-ssl
      port: 8883
      targetPort: 8883
      protocol: TCP
---
# --- L7: UI, REST API and UI live-update WebSocket ----------------------------
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: tbmq-ui
  namespace: thingsboard-mqtt-broker
spec:
  parentRefs:
    - name: edge
      namespace: envoy-gateway-system
  rules:
    # TBMQ serves /actuator (Prometheus metrics, health) without authentication: in-cluster only.
    - matches:
        - path:
            type: PathPrefix
            value: /actuator
      filters:
        - type: ExtensionRef
          extensionRef:
            group: gateway.envoyproxy.io
            kind: HTTPRouteFilter
            name: tbmq-actuator-deny
    - matches:
        - path:
            type: PathPrefix
            value: /api/ws
      backendRefs:
        - name: tbmq
          port: 8083
      timeouts:
        request: 0s          # long-lived WebSocket
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - name: tbmq
          port: 8083
---
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: HTTPRouteFilter
metadata:
  name: tbmq-actuator-deny
  namespace: thingsboard-mqtt-broker
spec:
  directResponse:
    statusCode: 404
    body:
      type: Inline
      inline: "not found"
