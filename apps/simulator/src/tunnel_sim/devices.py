"""Physical identity of a sensor device: MAC address and serial number.

Both are derived from the logical sensor id and the run's seed, so a device keeps the same
address across restarts without any state to store. The MAC is locally administered
(02:...), which is the range reserved for addresses that are not vendor-assigned.

`sensor_id` stays the identity used for topics, ACLs and the client certificate CN; the
device address is what the payload reports as the hardware that produced a detection.
"""

from __future__ import annotations

from hashlib import blake2b

SERIAL_ALPHABET = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ"   # no I/O: unambiguous on a label


def _digest(sensor_id: str, seed: int | None, kind: bytes, size: int) -> bytes:
    return blake2b(f"{seed}:{sensor_id}".encode(), digest_size=size, person=kind).digest()


def mac_for(sensor_id: str, seed: int | None) -> str:
    raw = bytearray(_digest(sensor_id, seed, b"mac", 6))
    raw[0] = 0x02                            # locally administered, unicast (02:... by convention)
    return ":".join(f"{b:02x}" for b in raw)


def serial_for(sensor_id: str, seed: int | None) -> str:
    n = int.from_bytes(_digest(sensor_id, seed, b"serial", 8), "big")
    out = []
    for _ in range(10):
        n, i = divmod(n, len(SERIAL_ALPHABET))
        out.append(SERIAL_ALPHABET[i])
    return "TSN-" + "".join(out)
