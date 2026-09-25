#!/usr/bin/env python3
"""Dependency-free network probes used by scripts/verify.sh.

  probe.py mqtt <host> <port> [--tls --ca FILE [--cert FILE --key FILE]] [--user U --password P]
                [--client-id ID] [--publish TOPIC]
      Sends an MQTT 5 CONNECT and prints the CONNACK reason code. With --publish, also sends a
      QoS 1 PUBLISH and prints the PUBACK reason code (or the broker's DISCONNECT).
      Exit 0: accepted, 2: reached the broker but refused (auth, ACL), 1: no broker / TLS failure.

  probe.py ws <host> <port> <path> [--subprotocol mqtt] [--tls --ca FILE]
      Sends a WebSocket upgrade, prints the HTTP status. Exit 0 on 101.
"""
import argparse
import base64
import os
import socket
import ssl
import struct
import sys

REASON_CODES = {
    0x00: "success",
    0x10: "no matching subscribers",
    0x80: "unspecified error",
    0x84: "unsupported protocol version",
    0x85: "client identifier not valid",
    0x86: "bad user name or password",
    0x87: "not authorized",
    0x88: "server unavailable",
    0x8A: "banned",
    0x8E: "session taken over",
    0x90: "topic name invalid",
    0x97: "quota exceeded",
    0x99: "payload format invalid",
}


def reason(code):
    return f"{code} ({REASON_CODES.get(code, 'unknown')})"


def connect(host, port, use_tls, ca, cert, key, timeout):
    sock = socket.create_connection((host, port), timeout=timeout)
    if not use_tls:
        return sock
    ctx = ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()
    if cert:
        ctx.load_cert_chain(cert, key)
    return ctx.wrap_socket(sock, server_hostname=host)


def utf8(s):
    b = s.encode()
    return struct.pack("!H", len(b)) + b


def varint(n):
    out = bytearray()
    while True:
        byte, n = n % 128, n // 128
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed by peer")
        buf += chunk
    return buf


def recv_packet(sock):
    """(packet type, body) of the next MQTT packet."""
    first = recv_exact(sock, 1)[0]
    length, shift = 0, 0
    while True:
        byte = recv_exact(sock, 1)[0]
        length |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    return first >> 4, recv_exact(sock, length)


def mqtt(args):
    flags = 0x02  # clean start
    payload = utf8(args.client_id or f"iot-verify-{os.getpid()}")
    if args.user:
        flags |= 0x80
        payload += utf8(args.user)
        if args.password is not None:
            flags |= 0x40
            payload += utf8(args.password)
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 30) + varint(0) + payload
    try:
        with connect(args.host, args.port, args.tls, args.ca, args.cert, args.key, args.timeout) as s:
            s.sendall(bytes([0x10]) + varint(len(body)) + body)
            ptype, data = recv_packet(s)
            if ptype != 2:
                print(f"unexpected packet type {ptype}")
                return 1
            code = data[1]
            print(f"CONNACK {reason(code)}")
            if code != 0:
                return 2
            if args.publish:
                body = utf8(args.publish) + struct.pack("!H", 1) + varint(0) + b"{}"
                s.sendall(bytes([0x32]) + varint(len(body)) + body)
                try:
                    ptype, data = recv_packet(s)
                except (ConnectionError, ssl.SSLError, OSError) as e:
                    print(f"PUBLISH {args.publish}: connection closed ({e})")
                    return 2
                if ptype == 14:  # DISCONNECT
                    print(f"PUBLISH {args.publish}: DISCONNECT {reason(data[0] if data else 0)}")
                    return 2
                if ptype != 4:
                    print(f"PUBLISH {args.publish}: unexpected packet type {ptype}")
                    return 1
                code = data[2] if len(data) > 2 else 0
                print(f"PUBACK {args.publish}: {reason(code)}")
                if code >= 0x80:
                    return 2
            s.sendall(b"\xe0\x00")  # DISCONNECT
            return 0
    except (OSError, ConnectionError, ssl.SSLError) as e:
        print(f"error: {e}")
        return 1


def ws(args):
    key = base64.b64encode(os.urandom(16)).decode()
    lines = [
        f"GET {args.path} HTTP/1.1",
        f"Host: {args.host}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    if args.subprotocol:
        lines.append(f"Sec-WebSocket-Protocol: {args.subprotocol}")
    request = ("\r\n".join(lines) + "\r\n\r\n").encode()
    try:
        with connect(args.host, args.port, args.tls, args.ca, None, None, args.timeout) as s:
            s.sendall(request)
            status = s.recv(1024).split(b"\r\n", 1)[0].decode(errors="replace")
            print(status)
            return 0 if " 101 " in f"{status} " else 2
    except (OSError, ssl.SSLError) as e:
        print(f"error: {e}")
        return 1


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--timeout", type=float, default=5.0)
    sub = p.add_subparsers(dest="cmd", required=True)

    m = sub.add_parser("mqtt")
    m.add_argument("host")
    m.add_argument("port", type=int)
    m.add_argument("--tls", action="store_true")
    m.add_argument("--ca")
    m.add_argument("--cert", help="client certificate chain (PEM) for mutual TLS")
    m.add_argument("--key", help="client private key (PEM)")
    m.add_argument("--user")
    m.add_argument("--password")
    m.add_argument("--client-id")
    m.add_argument("--publish", metavar="TOPIC", help="also publish a QoS 1 message to TOPIC")

    w = sub.add_parser("ws")
    w.add_argument("host")
    w.add_argument("port", type=int)
    w.add_argument("path")
    w.add_argument("--subprotocol")
    w.add_argument("--tls", action="store_true")
    w.add_argument("--ca")

    args = p.parse_args()
    sys.exit(mqtt(args) if args.cmd == "mqtt" else ws(args))


if __name__ == "__main__":
    main()
