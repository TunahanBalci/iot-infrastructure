#!/usr/bin/env python3
"""Render apps/simulator/config/tunnels.yaml into ClickHouse INSERTs (50-profiles.sql).

    scripts/render_profiles_sql.py apps/simulator/config/tunnels.yaml > 50-profiles.sql

The YAML is the single source of truth for the simulated tunnels; this puts the same names,
speed limits and access rules into iot.tunnel_profiles / iot.tunnel_rules so the alert views
(30-views.sql) judge the traffic by exactly the rules the devices are driving.

Rules are stored as local-minute windows (Europe/Istanbul); from >= to wraps midnight.
Applied by scripts/install/85-storage.sh whenever the rendered file changes.
"""

from __future__ import annotations

import re
import sys

import yaml

TIME = re.compile(r"^([01]\d|2[0-4]):([0-5]\d)$")


def minutes(value: str) -> int:
    m = TIME.match(str(value))
    if m is None:
        raise SystemExit(f"bad time {value!r}: expected HH:MM")
    return int(m.group(1)) * 60 + int(m.group(2))


def quote(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def main(path: str) -> int:
    with open(path) as f:
        tunnels = (yaml.safe_load(f) or {}).get("tunnels") or []
    if not tunnels:
        raise SystemExit(f"{path}: no tunnels defined")

    profiles, rules = [], []
    for t in tunnels:
        profiles.append("({}, {}, {}, {}, {}, {}, {})".format(
            quote(t["tunnel_id"]), quote(t["name"]), quote(t["city"]), int(t["city_code"]),
            float(t["length_m"]), int(t["lanes_per_direction"]), float(t["speed_limit_kmh"])))
        for rule in t.get("rules") or []:
            start, end = minutes(rule["from"]), minutes(rule["to"])
            for vehicle_type in rule["types"]:
                rules.append(f"({quote(t['tunnel_id'])}, {quote(vehicle_type)}, {start}, {end})")

    out = [
        "-- Generated from {} by scripts/render_profiles_sql.py — do not edit.".format(path),
        "-- ReplacingMergeTree(updated_at): re-applying replaces a tunnel's row; read with FINAL.",
        "",
        "INSERT INTO iot.tunnel_profiles",
        "    (tunnel_id, name, city, city_code, length_m, lanes_per_direction, speed_limit_kmh)",
        "VALUES",
        ",\n".join("    " + v for v in profiles) + ";",
        "",
    ]
    if rules:
        out += [
            "INSERT INTO iot.tunnel_rules (tunnel_id, vehicle_type, from_minute, to_minute)",
            "VALUES",
            ",\n".join("    " + v for v in rules) + ";",
            "",
        ]
    # Tunnels that disappeared from the YAML must stop matching the alert views.
    ids = ", ".join(quote(t["tunnel_id"]) for t in tunnels)
    out += [
        f"DELETE FROM iot.tunnel_profiles WHERE tunnel_id NOT IN ({ids});",
        f"DELETE FROM iot.tunnel_rules WHERE tunnel_id NOT IN ({ids});",
        "",
    ]
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    sys.exit(main(sys.argv[1]))
