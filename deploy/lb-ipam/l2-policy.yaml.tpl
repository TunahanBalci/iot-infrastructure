apiVersion: cilium.io/v2alpha1
kind: CiliumL2AnnouncementPolicy
metadata:
  name: edge-l2
  labels:
    app.kubernetes.io/managed-by: iot-infrastructure
spec:
  loadBalancerIPs: true
  interfaces:
${L2_INTERFACES_YAML}
