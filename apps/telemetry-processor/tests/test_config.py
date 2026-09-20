import pytest

from telemetry_processor.config import Config, apply_env_overrides, load_config
from telemetry_processor.service import consumer_config, producer_config


def test_default_config_file_matches_model_defaults():
    assert load_config("config/telemetry-processor.yaml", environ={}) == Config()


def test_defaults_match_contract():
    k = Config().kafka
    assert (k.group_id, k.input_topic, k.output_topic, k.rejected_topic) == (
        "telemetry-processor", "iot.mqtt.ingest", "iot.detections", "iot.detections.rejected")
    assert Config().service.http_port == 8080


def test_env_overrides_cluster_settings():
    env = {
        "PROCESSOR__KAFKA__BOOTSTRAP_SERVERS": "app-kafka-kafka-bootstrap.iot-pipeline.svc:9093",
        "PROCESSOR__KAFKA__SECURITY_PROTOCOL": "SASL_SSL",
        "PROCESSOR__KAFKA__SASL_USERNAME": "telemetry-processor",
        "PROCESSOR__KAFKA__SASL_PASSWORD": "0123456789",
        "PROCESSOR__KAFKA__SSL_CA_LOCATION": "/etc/app-kafka/ca.crt",
        "PROCESSOR__KAFKA__POLL_BATCH": "1000",
        "PROCESSOR__SERVICE__STATS_INTERVAL_S": "30",
        "PROCESSOR__LOGGING__LEVEL": "DEBUG",
        "UNRELATED": "x",
    }
    cfg = load_config("config/telemetry-processor.yaml", environ=env)
    k = cfg.kafka
    assert k.bootstrap_servers == "app-kafka-kafka-bootstrap.iot-pipeline.svc:9093"
    assert k.sasl_password == "0123456789"  # numeric-looking password stays a string
    assert k.poll_batch == 1000 and cfg.service.stats_interval_s == 30 and cfg.logging.level == "DEBUG"
    conf = consumer_config(k)
    assert conf["security.protocol"] == "SASL_SSL" and conf["sasl.mechanism"] == "SCRAM-SHA-512"
    assert conf["sasl.username"] == "telemetry-processor" and conf["ssl.ca.location"] == "/etc/app-kafka/ca.crt"
    assert cfg.redacted()["kafka"]["sasl_password"] == "***"


def test_empty_env_value_is_null():
    cfg = Config.model_validate(apply_env_overrides({}, {"PROCESSOR__SERVICE__HTTP_PORT": ""}))
    assert cfg.service.http_port is None


def test_librdkafka_passthrough_from_env_and_yaml():
    data = {"kafka": {"producer": {"linger.ms": 20, "enable.idempotence": True}}}
    env = {"PROCESSOR__KAFKA__CONSUMER__FETCH_WAIT_MAX_MS": "100",
           "PROCESSOR__KAFKA__CONSUMER__AUTO_OFFSET_RESET": "latest",
           "PROCESSOR__KAFKA__PRODUCER__COMPRESSION_TYPE": "zstd"}
    k = Config.model_validate(apply_env_overrides(data, env)).kafka
    assert k.consumer == {"fetch.wait.max.ms": "100", "auto.offset.reset": "latest"}
    assert k.producer == {"linger.ms": "20", "enable.idempotence": "true", "compression.type": "zstd"}
    c, p = consumer_config(k), producer_config(k)
    assert c["auto.offset.reset"] == "latest" and c["fetch.wait.max.ms"] == "100"
    assert p["compression.type"] == "zstd" and p["linger.ms"] == "20"


def test_builtin_client_settings():
    k = Config().kafka
    c, p = consumer_config(k), producer_config(k)
    assert c["partition.assignment.strategy"] == "cooperative-sticky"
    assert c["enable.auto.commit"] is False and c["enable.auto.offset.store"] is False
    assert c["group.id"] == "telemetry-processor" and "sasl.username" not in c
    assert (p["enable.idempotence"], p["acks"], p["compression.type"], p["partitioner"]) == (
        True, "all", "lz4", "murmur2_random")


def test_client_id_hostname_placeholder():
    k = Config().kafka
    assert k.resolved_client_id("telemetry-processor-7d9f-abcde") == "telemetry-processor-telemetry-processor-7d9f-abcde"
    with pytest.raises(ValueError):
        Config.model_validate({"kafka": {"client_id": "{pod}"}})


@pytest.mark.parametrize("kafka", [
    {"security_protocol": "SASL_SSL"},                                   # no credentials
    {"security_protocol": "TLS"},
    {"output_topic": "iot.mqtt.ingest"},
    {"poll_batch": 0},
    {"unknown_key": 1},
])
def test_invalid_config(kafka):
    with pytest.raises(ValueError):
        Config.model_validate({"kafka": kafka})


def test_missing_explicit_config_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nope.yaml", environ={})
    with pytest.raises(FileNotFoundError):
        load_config(None, environ={"PROCESSOR_CONFIG": str(tmp_path / "nope.yaml")})
