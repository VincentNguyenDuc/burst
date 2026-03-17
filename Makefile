.PHONY: controller submit status cluster-up cluster-down cluster-status clean

CMD ?= /bin/echo hello-from-burst
ARGV ?=
JOB_ID ?=

BURST_STATE_DIR ?= .burst-dev
CONFIG_PATH ?= burst-example.config.json
OUTPUT_DIR ?= ./.burst-dev/job-outputs

OUTPUT_DIR_ARG = $(if $(OUTPUT_DIR),--output-dir $(OUTPUT_DIR),)
SUBMIT_TOKENS = $(if $(ARGV),$(ARGV),$(CMD))
CONTROLLER_BIND = $(shell jq -r '.controller.bind_addr' "$(CONFIG_PATH)")
WORKER_IDS = $(shell jq -r '.workers[].worker_id' "$(CONFIG_PATH)")

controller:
	cargo run -p burst-controller -- --config "$(CONFIG_PATH)"

submit:
	cargo run -p burst-cli -- --config "$(CONFIG_PATH)" submit $(OUTPUT_DIR_ARG) $(SUBMIT_TOKENS)

status:
	cargo run -p burst-cli -- --config "$(CONFIG_PATH)" status --job-id "$(JOB_ID)"

cluster-up:
	mkdir -p "$(BURST_STATE_DIR)"
	echo "Starting controller on $(CONTROLLER_BIND)"
	nohup cargo run -p burst-controller -- --config "$(CONFIG_PATH)" >"$(BURST_STATE_DIR)/controller.log" 2>&1 & echo $$! >"$(BURST_STATE_DIR)/controller.pid"
	sleep 1
	for wid in $(WORKER_IDS); do \
		echo "Starting $$wid"; \
		nohup cargo run -p burst-worker -- --config "$(CONFIG_PATH)" --worker-id "$$wid" >"$(BURST_STATE_DIR)/$$wid.log" 2>&1 & echo $$! >"$(BURST_STATE_DIR)/$$wid.pid"; \
	done
	echo "Cluster started."

cluster-status:
	@state="$(BURST_STATE_DIR)"; \
	echo "Controller:"; \
	cat "$$state/controller.pid" 2>/dev/null || echo "no pid"; \
	echo "Workers:"; \
	ls -1 "$$state"/*.pid 2>/dev/null | xargs -I {} basename {} .pid | grep -v controller || echo "none"

cluster-down:
	@state="$(BURST_STATE_DIR)"; \
	echo "Stopping cluster processes..."; \
	for pidf in "$$state"/*.pid; do \
		[ -e "$$pidf" ] || continue; \
		pid=$$(cat "$$pidf"); \
		kill "$$pid" 2>/dev/null || true; \
		rm -f "$$pidf"; \
	done; \
	echo "Cluster stopped."

clean:
	rm -rf "$(BURST_STATE_DIR)"
	cargo clean
