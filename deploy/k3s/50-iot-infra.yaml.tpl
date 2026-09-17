# Managed by iot-infrastructure (scripts/install/10-k3s.sh) — do not edit here.
# Cilium replaces flannel, kube-proxy and network policy; Envoy Gateway
# replaces traefik; Cilium LB-IPAM replaces servicelb (klipper).
flannel-backend: none
disable-network-policy: true
disable-kube-proxy: true
disable:
  - traefik
  - servicelb
cluster-cidr: ${K3S_CLUSTER_CIDR}
service-cidr: ${K3S_SERVICE_CIDR}
# Stable address on dummy interface ${K3S_NODE_IP_IFACE} (iot-node-ip.service): node InternalIP,
# API server advertise address, kubelet serving certificate. Independent of the Wi-Fi address.
node-ip: ${K3S_NODE_IP}
