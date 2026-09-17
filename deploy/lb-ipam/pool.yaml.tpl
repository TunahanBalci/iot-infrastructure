apiVersion: cilium.io/v2
kind: CiliumLoadBalancerIPPool
metadata:
  name: edge-pool
  labels:
    app.kubernetes.io/managed-by: iot-infrastructure
spec:
  blocks:
    - start: ${LB_POOL_START}
      stop: ${LB_POOL_STOP}
