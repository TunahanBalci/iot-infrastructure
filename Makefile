# =============================================================================
# IoT infrastructure: k3s + Cilium (CNI, L4 MQTTS LB) + Envoy Gateway (L7) + TBMQ on Strimzi Kafka,
# CloudNativePG and Valkey → integration executor → app Kafka → telemetry-processor / consensus (KEDA)
# → ClickHouse + SeaweedFS; VictoriaMetrics/Logs/Traces, Grafana, OpenTelemetry
# =============================================================================
# Configuration: config.env (defaults) < config.local.env < make VAR=value
# Every target is a thin wrapper around scripts/ so each step also runs standalone.
# =============================================================================

SHELL        := /usr/bin/env bash
.SHELLFLAGS  := -eu -o pipefail -c
.DEFAULT_GOAL := help
.NOTPARALLEL:
# Pass command-line overrides (make install TBMQ_REPLICAS=3) to the scripts.
.EXPORT_ALL_VARIABLES:
MAKEFLAGS    += --no-print-directory

SCRIPTS := scripts
INSTALL := $(SCRIPTS)/install

INSTALL_STEPS := prereqs k3s cilium lb-ipam cert-manager envoy-gateway operators tbmq-deps app-kafka tbmq processing storage observability
VERIFY_STEPS  := $(INSTALL_STEPS) e2e

##@ General

.PHONY: help
help: ## Show this help
	@awk 'BEGIN { FS = ":.*##"; printf "\nUsage: make \033[36m<target>\033[0m [VAR=value ...]\n" } \
	  /^[a-zA-Z0-9_%-]+:.*##/ { printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2 } \
	  /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)
	@printf '\nInstall steps (in order): %s\n' "$(INSTALL_STEPS)"
	@printf 'Examples:\n'
	@printf '  make install                       # everything, idempotent\n'
	@printf '  make install-tbmq TBMQ_REPLICAS=3  # one step with an override\n'
	@printf '  make -k verify-cilium verify-e2e   # selected checks (-k: keep going on failure)\n'
	@printf '  make up SIM=true                   # start stack + simulator\n'
	@printf '  make sim-up MODE=generator LOAD=50000   # load test instead of the profile tunnels\n\n'

.PHONY: config
config: ## Print effective configuration
	@source $(SCRIPTS)/lib.sh; for v in $$CONFIG_NAMES; do \
	  val="$${!v}"; [[ "$$v" == *PASSWORD && -n "$$val" ]] && val='********'; printf '  %-28s %s\n' "$$v" "$$val"; done

.PHONY: lint
lint: ## Syntax-check all scripts (bash -n, shellcheck if installed)
	@for f in $(SCRIPTS)/*.sh $(INSTALL)/*.sh; do bash -n "$$f" || exit 1; done
	@for f in $(SCRIPTS)/*.py; do python3 -c "import ast,sys; ast.parse(open(sys.argv[1]).read())" "$$f" || exit 1; done
	@if command -v shellcheck >/dev/null; then shellcheck -x -S warning $(SCRIPTS)/*.sh $(INSTALL)/*.sh; \
	  else echo "shellcheck not installed — ran bash -n only"; fi
	@echo "lint ok"

##@ Install (idempotent — safe to re-run)

.PHONY: install
install: $(addprefix install-,$(INSTALL_STEPS)) ## Install everything, step by step
	@printf '\n\033[32mInstall complete.\033[0m Next: make verify\n'
	@$(SCRIPTS)/endpoints.sh

.PHONY: install-prereqs
install-prereqs: ## 00 · Check host tools, install helm/cilium/hubble CLIs
	@$(INSTALL)/00-prereqs.sh

.PHONY: install-k3s
install-k3s: ## 10 · k3s without flannel/kube-proxy/traefik/servicelb, node IP pinned to a dummy interface
	@$(INSTALL)/10-k3s.sh

.PHONY: install-cilium
install-cilium: ## 20 · Cilium CNI, kube-proxy replacement, Maglev L4 LB, Hubble
	@$(INSTALL)/20-cilium.sh

.PHONY: install-lb-ipam
install-lb-ipam: ## 30 · LoadBalancer IP pool (+ L2 announcements if enabled)
	@$(INSTALL)/30-lb-ipam.sh

.PHONY: install-cert-manager
install-cert-manager: ## 40 · cert-manager + server CA and device CA issuers
	@$(INSTALL)/40-cert-manager.sh

.PHONY: install-envoy-gateway
install-envoy-gateway: ## 50 · Gateway API CRDs, Envoy Gateway, edge Gateway
	@$(INSTALL)/50-envoy-gateway.sh

.PHONY: install-operators
install-operators: ## 55 · Strimzi (Kafka), CloudNativePG (Postgres), Reloader, KEDA
	@$(INSTALL)/55-operators.sh

.PHONY: install-tbmq-deps
install-tbmq-deps: ## 60 · Strimzi Kafka, CNPG Postgres, Valkey + one-time DB schema
	@$(INSTALL)/60-tbmq-deps.sh

.PHONY: install-app-kafka
install-app-kafka: ## 65 · App Kafka (Strimzi, TLS + SCRAM, ACLs): topics and users
	@$(INSTALL)/65-app-kafka.sh

.PHONY: install-tbmq
install-tbmq: ## 70 · TBMQ broker, MQTTS (mTLS), X.509 auth, UI route, integration → app Kafka (KEDA)
	@$(INSTALL)/70-tbmq.sh

.PHONY: install-processing
install-processing: ## 80 · telemetry-processor + consensus on app Kafka, KEDA ScaledObjects
	@$(INSTALL)/80-processing.sh

.PHONY: install-storage
install-storage: ## 85 · ClickHouse (Kafka ingestion, rollups) + SeaweedFS cold tier (STORAGE_ENABLED)
	@$(INSTALL)/85-storage.sh

.PHONY: install-observability
install-observability: ## 90 · VictoriaMetrics, vmalert, Grafana, VictoriaLogs + Vector, VictoriaTraces + OTel (OBSERVABILITY_ENABLED)
	@$(INSTALL)/90-observability.sh

##@ Verify (read-only)

.PHONY: verify
verify: ## Verify all parts, step by step (exit 1 on any failure)
	@$(SCRIPTS)/verify.sh $(VERIFY_STEPS)

# Pattern rule (not .PHONY: phony targets skip pattern matching). verify.sh validates the step name.
verify-%: ## Verify one step: verify-<step>, e.g. verify-cilium, verify-e2e
	@$(SCRIPTS)/verify.sh $*

##@ Lifecycle

.PHONY: up
up: ## Start k3s and all workloads in order (SIM=true also starts simulator)
	@$(SCRIPTS)/up.sh

.PHONY: down
down: ## Scale TBMQ stack to 0 (cluster keeps running, data kept)
	@$(SCRIPTS)/down.sh

.PHONY: stop
stop: ## down + stop k3s and all its containers
	@$(SCRIPTS)/down.sh --stop

.PHONY: restart
restart: down up ## down, then up

.PHONY: status
status: ## Show nodes, platform pods, LoadBalancers, routes, simulator
	@source $(SCRIPTS)/lib.sh; \
	  systemctl is-active --quiet k3s && ok "k3s active" || { warn "k3s not running"; exit 0; }; \
	  api_ready || { warn "API not ready"; exit 0; }; \
	  step "nodes"; kubectl get nodes -o wide; \
	  step "LoadBalancer services"; kubectl get svc -A --field-selector spec.type=LoadBalancer; \
	  step "gateway / routes"; kubectl get gateway,httproute -A; \
	  step "pods"; kubectl get pods -n kube-system -l 'k8s-app in (cilium,hubble-relay)' ; \
	  kubectl get pods -n cert-manager; kubectl get pods -n envoy-gateway-system; \
	  kubectl get pods -n strimzi-system; kubectl get pods -n cnpg-system; kubectl get pods -n reloader; \
	  step "TBMQ data services"; kubectl -n "$$TBMQ_NAMESPACE" get kafka,kafkanodepool,clusters.postgresql.cnpg.io,statefulset/tbmq-valkey; \
	  kubectl get pods -n "$$TBMQ_NAMESPACE"; \
	  step "pipeline"; kubectl -n iot-pipeline get kafka,kafkatopic,deploy,scaledobject 2>/dev/null || true; \
	  kubectl get pods -n iot-storage 2>/dev/null || true; kubectl get pods -n monitoring 2>/dev/null || true; kubectl get pods -n keda; \
	  step "simulator"; if command -v docker >/dev/null; then $(SCRIPTS)/sim.sh status; fi

.PHONY: endpoints
endpoints: ## Print URLs for UI and MQTTS
	@$(SCRIPTS)/endpoints.sh

##@ Simulator

.PHONY: sim-up
sim-up: ## Start simulator on the profile tunnels (MODE=generator LOAD=50000 for a load test)
	@MODE="$(MODE)" LOAD="$(LOAD)" $(SCRIPTS)/sim.sh up

.PHONY: sim-down
sim-down: ## Stop and remove simulator container
	@$(SCRIPTS)/sim.sh down

.PHONY: device-certs
device-certs: ## Issue client certificates for the simulated tunnels + iot-viewer (idempotent)
	@MODE="$(MODE)" $(SCRIPTS)/device-certs.sh

.PHONY: sim-logs
sim-logs: ## Follow simulator logs
	@$(SCRIPTS)/sim.sh logs

##@ Pipeline

.PHONY: consensus-test
consensus-test: ## Run consensus unit + simulator end-to-end tests locally (needs its Python deps)
	@cd apps/consensus && python3 -m pytest -q

.PHONY: logs-consensus
logs-consensus: ## Follow consensus logs (all replicas)
	@source $(SCRIPTS)/lib.sh; kubectl -n iot-pipeline logs -f -l app=consensus --max-log-requests 10 --prefix --tail 50

.PHONY: logs-processor
logs-processor: ## Follow telemetry-processor logs (all replicas)
	@source $(SCRIPTS)/lib.sh; kubectl -n iot-pipeline logs -f -l app=telemetry-processor --max-log-requests 10 --prefix --tail 50

.PHONY: clickhouse-client
clickhouse-client: ## ClickHouse SQL shell (database iot)
	@source $(SCRIPTS)/lib.sh; kubectl -n "$$STORAGE_NAMESPACE" exec -it clickhouse-0 -c clickhouse -- clickhouse-client --database iot

.PHONY: logs-clickhouse
logs-clickhouse: ## Follow ClickHouse logs
	@source $(SCRIPTS)/lib.sh; kubectl -n "$$STORAGE_NAMESPACE" logs -f statefulset/clickhouse --tail 50

.PHONY: watch-topic
watch-topic: ## Print records of an app Kafka topic live: make watch-topic TOPIC=iot.vehicles
	@$(SCRIPTS)/kafka-watch.sh "$${TOPIC:-iot.vehicles}"

.PHONY: mqtt-watch
mqtt-watch: ## Print device detections live (mosquitto_sub over mTLS with the iot-viewer certificate)
	@source $(SCRIPTS)/lib.sh; has mosquitto_sub || die "mosquitto_sub missing (apt install mosquitto-clients)"; \
	  $(SCRIPTS)/device-certs.sh 0 >/dev/null; \
	  mosquitto_sub -h "$$MQTT_LB_IP" -p 8883 -V mqttv5 --cafile "$$DEVICE_CERTS_DIR/server-ca.crt" \
	    --cert "$$DEVICE_CERTS_DIR/iot-viewer.pem" --key "$$DEVICE_CERTS_DIR/iot-viewer.key" \
	    -t 'tunnels/+/sensors/+/detections' -v

##@ Debug

.PHONY: cilium-status
cilium-status: ## cilium status + L4 service table for the LB IPs
	@source $(SCRIPTS)/lib.sh; cilium status; \
	  step "Cilium service table (LB IPs)"; \
	  kubectl -n kube-system exec ds/cilium -c cilium-agent -- cilium-dbg service list | grep -A4 -E "$$EDGE_LB_IP|$$MQTT_LB_IP" || true

.PHONY: hubble-ui
hubble-ui: ## Open Hubble UI (port-forward)
	@source $(SCRIPTS)/lib.sh; cilium hubble ui

.PHONY: hubble-mqtt
hubble-mqtt: ## Live flow log for MQTT traffic (ports 1883/8883)
	@source $(SCRIPTS)/lib.sh; \
	  { cilium hubble port-forward >/dev/null 2>&1 & pf=$$!; trap 'kill $$pf' EXIT; sleep 3; \
	    hubble observe --follow --port 1883 --port 8883; }

.PHONY: logs-tbmq
logs-tbmq: ## Follow TBMQ broker logs (all replicas)
	@source $(SCRIPTS)/lib.sh; kubectl -n "$$TBMQ_NAMESPACE" logs -f -l app=tbmq --max-log-requests 10 --prefix --tail 50
