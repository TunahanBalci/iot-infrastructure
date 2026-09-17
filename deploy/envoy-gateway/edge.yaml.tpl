# L7 edge: Envoy Gateway data plane exposed through a Cilium LB-IPAM address.
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: EnvoyProxy
metadata:
  name: edge-proxy
  namespace: envoy-gateway-system
spec:
  provider:
    type: Kubernetes
    kubernetes:
      envoyDeployment:
        replicas: ${ENVOY_REPLICAS}
        pod:
          affinity:
            podAntiAffinity:
              preferredDuringSchedulingIgnoredDuringExecution:
                - weight: 100
                  podAffinityTerm:
                    topologyKey: kubernetes.io/hostname
                    labelSelector:
                      matchLabels:
                        app.kubernetes.io/name: envoy
      envoyService:
        type: LoadBalancer
        externalTrafficPolicy: Cluster
        # Cilium serves the LB IP directly; NodePorts would expose the UI on the host's Wi-Fi IP.
        allocateLoadBalancerNodePorts: false
        annotations:
          lbipam.cilium.io/ips: "${EDGE_LB_IP}"
---
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: envoy
spec:
  controllerName: gateway.envoyproxy.io/gatewayclass-controller
  parametersRef:
    group: gateway.envoyproxy.io
    kind: EnvoyProxy
    name: edge-proxy
    namespace: envoy-gateway-system
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: edge-tls
  namespace: envoy-gateway-system
spec:
  secretName: edge-tls
  commonName: "*.${DOMAIN}"
  dnsNames:
    - "${DOMAIN}"
    - "*.${DOMAIN}"
  ipAddresses:
    - "${EDGE_LB_IP}"
  issuerRef:
    kind: ClusterIssuer
    name: iot-ca
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: edge
  namespace: envoy-gateway-system
spec:
  gatewayClassName: envoy
  listeners:
    - name: http
      protocol: HTTP
      port: 80
      allowedRoutes:
        namespaces:
          from: All
    - name: https
      protocol: HTTPS
      port: 443
      tls:
        mode: Terminate
        certificateRefs:
          - name: edge-tls
      allowedRoutes:
        namespaces:
          from: All
