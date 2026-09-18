#!/usr/bin/env python3
"""Idempotent TBMQ MQTT authentication setup over the REST API (stdlib only).

  tbmq_credentials.py ensure-x509 --url http://172.30.255.10 --file device-credentials.json
      Certificates are the only way into MQTT: the X.509 provider enabled with client certificates
      required, the credentials in FILE, and MQTT Basic shut down — provider disabled and every
      MQTT Basic credentials object deleted, including TBMQ's built-in WebSocket credentials, which
      have no password and allow every topic. At least one provider stays enabled: with none, TBMQ
      accepts every client.

  tbmq_credentials.py check-x509 --url http://172.30.255.10 --file device-credentials.json
      Read-only check of the same state. Exit 0: in place, 2: differs (reasons printed).

  tbmq_credentials.py admin-password --url http://172.30.255.10
      Makes $TBMQ_ADMIN_PASSWORD the admin password: logs in with it, or with the TBMQ install
      default and changes it. Exit 0: in place, 3: neither password is accepted.

  tbmq_credentials.py ensure-integration --url http://172.30.255.10 --file app-kafka-integration.json [--id-file F]
  tbmq_credentials.py check-integration  --url http://172.30.255.10 --file app-kafka-integration.json
      Kafka integration from FILE (matched by name), with the SCRAM login from $APP_KAFKA_USERNAME /
      $APP_KAFKA_PASSWORD and the CA PEM from $APP_KAFKA_CA_PEM added to otherProperties. ensure: create or
      update, print and optionally write the integration id. check: exit 0 when equal and enabled, 2 when not.

  tbmq_credentials.py prune-integrations --url http://172.30.255.10 --prefix app-kafka-ingest- NAME...
  tbmq_credentials.py check-integrations --url http://172.30.255.10 --prefix app-kafka-ingest- NAME...
      The integrations whose name starts with PREFIX must be exactly NAMES: a changed TBMQ_IE_SHARDS
      renames them, and the ones left over keep consuming their topics. prune: delete the leftovers.
      check: exit 0 when the set matches, 2 when it does not (leftover and missing names printed).

  tbmq_credentials.py check-admin --url http://172.30.255.10
      Read-only: the configured password works, the default one does not, and a token re-signed
      with TBMQ's public default JWT key is rejected. Exit 0: all true, 2: not (reasons printed).

Admin login comes from $TBMQ_ADMIN_USER / $TBMQ_ADMIN_PASSWORD. Passwords are read from
the environment so they never show up in `ps`. Exit 1: error (including TBMQ not answering yet).
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

X509_PROVIDER_CONFIG = {"clientAuthType": "CLIENT_AUTH_REQUIRED", "skipValidityCheckForClientCert": False}
AUTH_SETTINGS_KEY = "mqttAuthorization"
DEFAULT_PRIORITIES = ["MQTT_BASIC", "X_509", "JWT", "HTTP"]
TBMQ_DEFAULT_ADMIN_PASSWORD = "sysadmin"
# Created by the TBMQ installer for the UI's WebSocket client: no password and allow-all rules, and TBMQ
# refuses to delete it ("System WebSocket MQTT client credentials can not be deleted"). Disabling the
# MQTT Basic provider is what stops it from authenticating.
TBMQ_SYSTEM_WS_CREDENTIALS_NAME = "TBMQ WebSockets MQTT Credentials"
# JWT signing key TBMQ 2.4.0 uses when JWT_TOKEN_SIGNING_KEY is unset (public: thingsboard-mqtt-broker.yml).
TBMQ_DEFAULT_JWT_SIGNING_KEY = "Qk1xUnloZ0VQTlF1VlNJQXZ4cWhiNWt1cVd1ZzQ5cWpENUhMSHlaYmZIM0JrZ2pPTVlhQ3N1Z0ZMUnd0SDBieg=="


class ApiError(Exception):
    pass


def request(url, method="GET", body=None, token=None, timeout=15):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("X-Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, json.loads(raw) if raw.strip() else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            detail = json.loads(raw).get("message", raw.decode(errors="replace"))
        except (ValueError, AttributeError):
            detail = raw.decode(errors="replace")
        return e.code, detail


def admin_env():
    admin_user, admin_password = os.environ.get("TBMQ_ADMIN_USER"), os.environ.get("TBMQ_ADMIN_PASSWORD")
    if not (admin_user and admin_password):
        raise ApiError("TBMQ_ADMIN_USER / TBMQ_ADMIN_PASSWORD not set")
    return admin_user, admin_password


def try_login(base, user, password):
    """Access token, or None when TBMQ rejects the credentials. Other failures raise."""
    status, body = request(f"{base}/api/auth/login", "POST", {"username": user, "password": password})
    if status == 200 and isinstance(body, dict) and "token" in body:
        return body["token"]
    if status == 401:
        return None
    raise ApiError(f"TBMQ admin login failed ({status}): {body}")


def login(base):
    admin_user, admin_password = admin_env()
    token = try_login(base, admin_user, admin_password)
    if token is None:
        raise ApiError(f"TBMQ admin login rejected for {admin_user} (TBMQ_ADMIN_PASSWORD)")
    return token


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def resign(token, key):
    """The same JWT header and claims, signed with another HMAC key."""
    header_b64, payload_b64, _ = token.split(".")
    alg = json.loads(base64.urlsafe_b64decode(header_b64 + "=" * (-len(header_b64) % 4)))["alg"]
    digest = {"HS256": hashlib.sha256, "HS384": hashlib.sha384, "HS512": hashlib.sha512}[alg]
    signature = hmac.new(key, f"{header_b64}.{payload_b64}".encode(), digest).digest()
    return f"{header_b64}.{payload_b64}.{b64url(signature)}"


def admin_password(base):
    admin_user, wanted = admin_env()
    if try_login(base, admin_user, wanted):
        print(f"admin password for {admin_user} already set")
        return 0
    token = None if wanted == TBMQ_DEFAULT_ADMIN_PASSWORD else try_login(base, admin_user, TBMQ_DEFAULT_ADMIN_PASSWORD)
    if token is None:
        print(f"TBMQ rejects both TBMQ_ADMIN_PASSWORD and the install default for {admin_user}: "
              f"set TBMQ_ADMIN_PASSWORD to the current password", file=sys.stderr)
        return 3
    call(base, token, "/api/auth/changePassword", "POST",
         {"currentPassword": TBMQ_DEFAULT_ADMIN_PASSWORD, "newPassword": wanted})
    if not try_login(base, admin_user, wanted):
        raise ApiError("password change reported success, but the new password is rejected")
    print(f"admin password for {admin_user} changed from the TBMQ default")
    return 0


def check_admin(base):
    admin_user, wanted = admin_env()
    problems = []
    token = try_login(base, admin_user, wanted)
    if token is None:
        problems.append("configured admin password rejected")
    if wanted == TBMQ_DEFAULT_ADMIN_PASSWORD or try_login(base, admin_user, TBMQ_DEFAULT_ADMIN_PASSWORD):
        problems.append("TBMQ install default admin password accepted")
    if token:
        status, _ = request(f"{base}/api/auth/user", token=token)
        if status != 200:
            problems.append(f"genuine admin token rejected ({status})")
        forged = resign(token, base64.b64decode(TBMQ_DEFAULT_JWT_SIGNING_KEY))
        status, _ = request(f"{base}/api/auth/user", token=forged)
        if status == 200:
            problems.append("token signed with TBMQ's public default JWT key accepted (JWT_TOKEN_SIGNING_KEY unset)")
    for p in problems:
        print(p)
    return 2 if problems else 0


def call(base, token, path, method="GET", body=None, ok=(200,)):
    status, result = request(f"{base}{path}", method, body, token=token)
    if status not in ok:
        raise ApiError(f"{method} {path} failed ({status}): {result}")
    return status, result


def credentials_by_name(base, token, name):
    """Credentials object, or None when no credentials have that name."""
    status, existing = call(base, token, f"/api/mqtt/client/credentials?{urllib.parse.urlencode({'name': name})}",
                            ok=(200, 404))
    return existing if status == 200 and isinstance(existing, dict) else None


def all_credentials(base, token):
    """Every MQTT client credentials object."""
    out, page = [], 0
    while True:
        _, data = call(base, token, f"/api/mqtt/client/credentials?{urllib.parse.urlencode({'pageSize': 100, 'page': page})}")
        out += data.get("data", [])
        if not data.get("hasNext"):
            return out
        page += 1


def entity_id(obj):
    value = obj.get("id")
    return value.get("id") if isinstance(value, dict) else value


def x509_state(base, token, wanted):
    """Compare TBMQ with the wanted X.509 setup. Returns (problems, fixes): fixes are callables."""
    problems, fixes = [], []

    _, provider = call(base, token, "/api/mqtt/auth/provider/type/X_509")
    config = provider.get("configuration") or {}
    if any(config.get(k) != v for k, v in X509_PROVIDER_CONFIG.items()):
        problems.append(f"X.509 provider configuration is {config}, want {X509_PROVIDER_CONFIG}")

        def save_provider():
            provider["configuration"] = {**config, "type": "X_509", **X509_PROVIDER_CONFIG}
            call(base, token, "/api/mqtt/auth/provider", "POST", provider)
            print(f"X.509 provider: clientAuthType={X509_PROVIDER_CONFIG['clientAuthType']}")
        fixes.append(save_provider)
    if not provider.get("enabled"):
        problems.append("X.509 provider is disabled")

        def enable_provider():
            call(base, token, f"/api/mqtt/auth/provider/{entity_id(provider)}/enable", "POST")
            print("X.509 provider enabled")
        fixes.append(enable_provider)

    # A client with a device certificate that also sends MQTT Basic credentials must get its
    # certificate identity, so X.509 goes first.
    status, settings = call(base, token, f"/api/admin/settings/{AUTH_SETTINGS_KEY}", ok=(200, 404))
    if status == 404 or not isinstance(settings, dict):
        settings = {"key": AUTH_SETTINGS_KEY, "jsonValue": {"priorities": DEFAULT_PRIORITIES}}
    priorities = (settings.get("jsonValue") or {}).get("priorities") or DEFAULT_PRIORITIES
    if priorities[:1] != ["X_509"]:
        problems.append(f"authentication priorities are {priorities}, want X_509 first")

        def save_priorities():
            settings["jsonValue"] = {**(settings.get("jsonValue") or {}),
                                     "priorities": ["X_509"] + [p for p in priorities if p != "X_509"]}
            call(base, token, "/api/admin/settings", "POST", settings)
            print(f"authentication priorities: {settings['jsonValue']['priorities']}")
        fixes.append(save_priorities)

    # MQTT Basic: no client uses it (devices present certificates, consensus consumes Kafka) and TBMQ's
    # built-in WebSocket credentials carry no password with allow-all rules, so with the provider enabled
    # any client reaching a listener could publish and subscribe to everything.
    _, basic_provider = call(base, token, "/api/mqtt/auth/provider/type/MQTT_BASIC")
    if basic_provider.get("enabled"):
        problems.append("MQTT Basic provider is enabled (no client needs it)")

        def disable_basic():
            call(base, token, f"/api/mqtt/auth/provider/{entity_id(basic_provider)}/disable", "POST")
            print("MQTT Basic provider disabled")
        fixes.append(disable_basic)

    basic_credentials = [c for c in all_credentials(base, token)
                         if c.get("credentialsType") == "MQTT_BASIC" and c.get("name") != TBMQ_SYSTEM_WS_CREDENTIALS_NAME]
    if basic_credentials:
        problems.append("MQTT Basic credentials exist: " + ", ".join(sorted(c["name"] for c in basic_credentials)))

        def delete_basic():
            for c in basic_credentials:
                call(base, token, f"/api/mqtt/client/credentials/{entity_id(c)}", "DELETE")
                print(f"deleted MQTT Basic credentials '{c['name']}'")
        fixes.append(delete_basic)

    name = wanted["name"]
    existing = credentials_by_name(base, token, name)
    desired = {k: wanted[k] for k in ("name", "clientType", "credentialsType")}
    if existing is None:
        problems.append(f"X.509 credentials '{name}' missing")

        def create_credentials():
            call(base, token, "/api/mqtt/client/credentials", "POST",
                 {**desired, "credentialsValue": json.dumps(wanted["credentialsValue"])})
            print(f"X.509 credentials '{name}' created")
        fixes.append(create_credentials)
    else:
        try:
            current_value = json.loads(existing.get("credentialsValue") or "{}")
        except ValueError:
            current_value = None
        if any(existing.get(k) != v for k, v in desired.items()) or current_value != wanted["credentialsValue"]:
            problems.append(f"X.509 credentials '{name}' differ from the wanted rules")

            def update_credentials():
                call(base, token, "/api/mqtt/client/credentials", "POST",
                     {**existing, **desired, "credentialsValue": json.dumps(wanted["credentialsValue"])})
                print(f"X.509 credentials '{name}' updated")
            fixes.append(update_credentials)
    return problems, fixes


def x509(a, base, apply):
    with open(a.file) as f:
        wanted = json.load(f)
    token = login(base)
    problems, fixes = x509_state(base, token, wanted)
    if not apply:
        for p in problems:
            print(p)
        return 2 if problems else 0
    for fix in fixes:
        fix()
    if not fixes:
        print(f"X.509 provider (client certificate required) and credentials '{wanted['name']}' already in place")
    return 0


def integration_config(path):
    """Integration from FILE with the Kafka secrets from the environment merged in."""
    with open(path) as f:
        wanted = json.load(f)
    wanted.pop("_comment", None)
    user, password, ca = (os.environ.get(v) for v in ("APP_KAFKA_USERNAME", "APP_KAFKA_PASSWORD", "APP_KAFKA_CA_PEM"))
    if not (user and password and ca):
        raise ApiError("APP_KAFKA_USERNAME / APP_KAFKA_PASSWORD / APP_KAFKA_CA_PEM not set")
    props = wanted["configuration"]["clientConfiguration"].setdefault("otherProperties", {})
    props["sasl.jaas.config"] = (f'org.apache.kafka.common.security.scram.ScramLoginModule required '
                                 f'username="{user}" password="{password}";')
    props["ssl.truststore.certificates"] = ca.strip() + "\n"
    return wanted


def integration_by_name(base, token, name):
    page = 0
    while True:
        _, data = call(base, token, f"/api/integrations?{urllib.parse.urlencode({'pageSize': 100, 'page': page, 'textSearch': name})}")
        for item in data.get("data", []):
            if item.get("name") == name:
                return item
        if not data.get("hasNext"):
            return None
        page += 1


def integrations_with_prefix(base, token, prefix):
    """{name: integration} of every integration whose name starts with PREFIX."""
    out, page = {}, 0
    while True:
        _, data = call(base, token, f"/api/integrations?{urllib.parse.urlencode({'pageSize': 100, 'page': page})}")
        out.update({i["name"]: i for i in data.get("data", []) if str(i.get("name", "")).startswith(prefix)})
        if not data.get("hasNext"):
            return out
        page += 1


def integration_differs(existing, wanted):
    return any(existing.get(k) != wanted[k] for k in ("type", "enabled", "configuration"))


def integration(a, base, apply):
    wanted = integration_config(a.file)
    token = login(base)
    existing = integration_by_name(base, token, wanted["name"])
    if existing and not integration_differs(existing, wanted):
        result, action = existing, "already in place"
    elif not apply:
        print(f"integration '{wanted['name']}' " + ("differs from the wanted configuration" if existing else "missing"))
        return 2
    else:
        body = {**existing, **wanted} if existing else wanted
        _, result = call(base, token, "/api/integration", "POST", body)
        action = "updated" if existing else "created"
    integration_id = entity_id(result)
    if apply:
        print(f"integration '{wanted['name']}' {action} (id {integration_id})")
        if a.id_file:
            with open(a.id_file, "w") as f:
                f.write(f"{integration_id}\n")
    return 0


def integration_set(a, base, apply):
    """The PREFIX integrations against the wanted names. Creating them is ensure-integration's job, so
    apply only deletes: an integration left over from an earlier shard count keeps consuming its topics."""
    token = login(base)
    found = integrations_with_prefix(base, token, a.prefix)
    leftover, missing = sorted(set(found) - set(a.name)), sorted(set(a.name) - set(found))
    if not apply:
        for name in missing:
            print(f"integration '{name}' missing")
        for name in leftover:
            print(f"integration '{name}' left over from an earlier shard count (not one of the {len(a.name)} wanted)")
        return 2 if leftover or missing else 0
    for name in leftover:
        call(base, token, f"/api/integration/{entity_id(found[name])}", "DELETE")
        print(f"deleted integration '{name}' (left over from an earlier shard count)")
    if not leftover:
        print(f"no leftover '{a.prefix}*' integrations")
    return 0


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="action", required=True)

    for action in ("admin-password", "check-admin"):
        sub.add_parser(action, help="TBMQ admin password / JWT key").add_argument(
            "--url", required=True, help="TBMQ base URL (UI/REST)")

    for action in ("ensure-integration", "check-integration"):
        i = sub.add_parser(action, help="Kafka integration (integration executor -> app Kafka)")
        i.add_argument("--url", required=True, help="TBMQ base URL (UI/REST)")
        i.add_argument("--file", required=True, help="integration JSON (deploy/tbmq/app-kafka-integration.json.tpl)")
        i.add_argument("--id-file", help="write the integration id here (ensure-integration)")

    for action in ("prune-integrations", "check-integrations"):
        s = sub.add_parser(action, help="the integration set of one name prefix (delete / report leftovers)")
        s.add_argument("--url", required=True, help="TBMQ base URL (UI/REST)")
        s.add_argument("--prefix", required=True, help="only integrations whose name starts with this are considered")
        s.add_argument("name", nargs="+", help="the integration names that must exist")

    for action in ("ensure-x509", "check-x509"):
        x = sub.add_parser(action, help="X.509 provider + certificate-chain credentials")
        x.add_argument("--url", required=True, help="TBMQ base URL (UI/REST)")
        x.add_argument("--file", required=True, help="credentials JSON (deploy/tbmq/device-credentials.json.tpl)")

    a = p.parse_args()
    base = a.url.rstrip("/")
    try:
        if a.action == "admin-password":
            return admin_password(base)
        if a.action == "check-admin":
            return check_admin(base)
        if a.action in ("ensure-integration", "check-integration"):
            return integration(a, base, apply=a.action == "ensure-integration")
        if a.action in ("prune-integrations", "check-integrations"):
            return integration_set(a, base, apply=a.action == "prune-integrations")
        return x509(a, base, apply=a.action == "ensure-x509")
    except ApiError as e:
        print(e, file=sys.stderr)
        return 1
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"TBMQ REST API unreachable at {base}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
