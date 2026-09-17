# Managed by iot-infrastructure (scripts/install/10-k3s.sh) — do not edit here.
# Stable k3s node address on a dummy interface: the host's Wi-Fi/DHCP address changes with the
# network, this one does not. Host-local /32, never announced or routed off this machine.
[Unit]
Description=iot-infrastructure: k3s node address ${K3S_NODE_IP} on dummy interface ${K3S_NODE_IP_IFACE}
Before=k3s.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/sh -c 'ip link show ${K3S_NODE_IP_IFACE} >/dev/null 2>&1 || ip link add ${K3S_NODE_IP_IFACE} type dummy; ip addr flush dev ${K3S_NODE_IP_IFACE}; ip addr add ${K3S_NODE_IP}/32 dev ${K3S_NODE_IP_IFACE}; ip link set ${K3S_NODE_IP_IFACE} up'

[Install]
WantedBy=multi-user.target
