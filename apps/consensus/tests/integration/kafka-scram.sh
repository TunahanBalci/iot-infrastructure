#!/usr/bin/env bash
# Local stand-in for the app Kafka (Strimzi app-kafka): Kafka 4.3.1, SASL_SSL + SCRAM-SHA-512 (1 GiB container).
#   kafka-scram.sh up <name> <host-port> <user:password>...   # also creates docker network <name>-net
#   kafka-scram.sh topic <name> <topic> <partitions> [compact]
#   kafka-scram.sh down <name>
# Clients: bootstrap 127.0.0.1:<host-port> (from host) or <name>:9093 (on network <name>-net),
# security_protocol SASL_SSL, sasl_mechanism SCRAM-SHA-512, CA file <dir>/ca.crt (printed on up).
# The broker certificate has SANs localhost, 127.0.0.1 and <name>.
set -euo pipefail
cmd=$1 name=$2
dir=${KAFKA_HARNESS_DIR:-${TMPDIR:-/tmp}/kafka-harness}/$name
case $cmd in
  up)
    port=$3; shift 3
    rm -rf "$dir"; mkdir -p "$dir"; cd "$dir"
    openssl req -x509 -new -newkey rsa:2048 -nodes -keyout ca.key -subj "/O=io.strimzi/CN=cluster-ca v0" -days 7 \
      -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign -out ca.crt 2>/dev/null
    openssl req -new -newkey rsa:2048 -nodes -keyout broker.key -subj "/CN=$name" -out broker.csr 2>/dev/null
    printf 'subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:%s\nbasicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nauthorityKeyIdentifier=keyid\n' "$name" >broker.ext
    openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -days 7 -extfile broker.ext -out broker.crt 2>/dev/null
    openssl pkcs12 -export -inkey broker.key -in broker.crt -certfile ca.crt -name broker -passout pass:brokerpw -out broker.p12
    scram=()
    for up in "$@"; do scram+=(--add-scram "SCRAM-SHA-512=[name=${up%%:*},password=${up#*:}]"); done
    cat >server.properties <<P
node.id=1
process.roles=broker,controller
listeners=SASL_SSL://:9093,EXTERNAL://:9095,INTERNAL://:9092,CONTROLLER://:9094
advertised.listeners=SASL_SSL://$name:9093,EXTERNAL://localhost:$port,INTERNAL://$name:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,SASL_SSL:SASL_SSL,EXTERNAL:SASL_SSL
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
controller.quorum.bootstrap.servers=$name:9094
sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.external.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
ssl.keystore.type=PKCS12
ssl.keystore.location=/kafka-local/broker.p12
ssl.keystore.password=brokerpw
ssl.key.password=brokerpw
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
log.dirs=/tmp/kraft-logs
P
    chmod 644 ./*
    docker network create "$name-net" >/dev/null 2>&1 || true
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker run -d --name "$name" --network "$name-net" -m 1g -p "127.0.0.1:$port:9095" -v "$dir:/kafka-local:ro" \
      -e "KAFKA_HEAP_OPTS=-Xmx384m -Xms384m" --entrypoint sh apache/kafka:4.3.1 -c \
      "/opt/kafka/bin/kafka-storage.sh format --standalone -t \$(/opt/kafka/bin/kafka-storage.sh random-uuid) -c /kafka-local/server.properties $(printf "'%s' " "${scram[@]}") >/dev/null && exec /opt/kafka/bin/kafka-server-start.sh /kafka-local/server.properties" >/dev/null
    for _ in $(seq 1 40); do
      docker logs "$name" 2>&1 | grep -q 'Kafka Server started' && { echo "kafka $name up: 127.0.0.1:$port (host) / $name:9093 (network $name-net), CA $dir/ca.crt"; exit 0; }
      sleep 1
    done
    docker logs "$name" 2>&1 | tail -5; exit 1
    ;;
  topic)
    topic=$3 parts=$4 extra=()
    [[ "${5:-}" == compact ]] && extra=(--config cleanup.policy=compact)
    docker exec "$name" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$name:9092" --create --if-not-exists \
      --topic "$topic" --partitions "$parts" --replication-factor 1 "${extra[@]}" >/dev/null && echo "topic $topic ($parts)"
    ;;
  down)
    docker rm -f "$name" >/dev/null 2>&1 || true; docker network rm "$name-net" >/dev/null 2>&1 || true; echo "down $name"
    ;;
esac
