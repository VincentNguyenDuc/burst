.PHONY: check controller submit status cluster-up cluster-down cluster-status

CMD ?= /bin/echo hello-from-burst
JOB_ID ?=
BURST_STATE_DIR ?= .burst-dev
CONFIG_PATH ?= burst.config.json
PYTHON ?= python3
NUM_WORKERS ?= $(shell $(PYTHON) -c 'import json; print(json.load(open("$(CONFIG_PATH)"))["cluster"]["num_workers"])')
WORKER_SLOTS ?= $(shell $(PYTHON) -c 'import json; print(json.load(open("$(CONFIG_PATH)"))["cluster"]["worker_slots"])')
CONTROLLER_ADDR ?= $(shell $(PYTHON) -c 'import json; print(json.load(open("$(CONFIG_PATH)"))["cli"]["controller_addr"])')
CONTROLLER_BIND ?= $(shell $(PYTHON) -c 'import json; print(json.load(open("$(CONFIG_PATH)"))["controller"]["bind_addr"])')

controller:
	cargo run -p burst-controller -- --config $(CONFIG_PATH)

submit:
	cargo run -p burst-cli -- --config $(CONFIG_PATH) --controller $(CONTROLLER_ADDR) submit $(CMD)

status:
	@if [ -z "$(JOB_ID)" ]; then echo "Usage: make status JOB_ID=job-00000001"; exit 1; fi
	cargo run -p burst-cli -- --config $(CONFIG_PATH) --controller $(CONTROLLER_ADDR) status --job-id $(JOB_ID)

cluster-up:
	@mkdir -p $(BURST_STATE_DIR)
	@echo "Starting controller on $(CONTROLLER_BIND)"
	@cargo run -p burst-controller -- --config $(CONFIG_PATH) > $(BURST_STATE_DIR)/controller.log 2>&1 & echo $$! > $(BURST_STATE_DIR)/controller.pid
	@sleep 1
	@i=1; while [ $$i -le $(NUM_WORKERS) ]; do \
		worker_id="worker-$$i"; \
		echo "Starting $$worker_id with slots=$(WORKER_SLOTS)"; \
		cargo run -p burst-worker -- --config $(CONFIG_PATH) --worker-id $$worker_id --slots $(WORKER_SLOTS) > $(BURST_STATE_DIR)/$$worker_id.log 2>&1 & echo $$! > $(BURST_STATE_DIR)/$$worker_id.pid; \
		i=$$((i + 1)); \
	done
	@echo "Cluster started. Use 'make cluster-status' to inspect."

cluster-status:
	@echo "Controller PID:"; if [ -f $(BURST_STATE_DIR)/controller.pid ]; then cat $(BURST_STATE_DIR)/controller.pid; else echo "not running"; fi
	@echo "Workers:"; ls $(BURST_STATE_DIR)/worker-*.pid 2>/dev/null || echo "none"

cluster-down:
	@echo "Stopping cluster processes..."
	@if [ -f $(BURST_STATE_DIR)/controller.pid ]; then kill $$(cat $(BURST_STATE_DIR)/controller.pid) 2>/dev/null || true; rm -f $(BURST_STATE_DIR)/controller.pid; fi
	@for pidf in $(BURST_STATE_DIR)/worker-*.pid; do \
		if [ -f $$pidf ]; then kill $$(cat $$pidf) 2>/dev/null || true; rm -f $$pidf; fi; \
	done
	@echo "Cluster stopped. Logs remain in $(BURST_STATE_DIR)/"
