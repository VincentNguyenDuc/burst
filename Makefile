.PHONY: check controller submit status cluster-up cluster-down cluster-status

CMD ?= /bin/echo hello-from-burst
JOB_ID ?=
BURST_STATE_DIR ?= .burst-dev
CONFIG_PATH ?= burst.config.json
PYTHON ?= python3

SCRIPTS_BURST ?= $(PYTHON) scripts/burst.py

controller:
	$(SCRIPTS_BURST) --config $(CONFIG_PATH) controller

submit:
	CMD="$(CMD)" $(SCRIPTS_BURST) --config $(CONFIG_PATH) submit

status:
	@if [ -z "$(JOB_ID)" ]; then echo "Usage: make status JOB_ID=job-00000001"; exit 1; fi
	$(SCRIPTS_BURST) --config $(CONFIG_PATH) status --job-id $(JOB_ID)

cluster-up:
	$(SCRIPTS_BURST) --config $(CONFIG_PATH) --state-dir $(BURST_STATE_DIR) cluster-up

cluster-status:
	$(SCRIPTS_BURST) --config $(CONFIG_PATH) --state-dir $(BURST_STATE_DIR) cluster-status

cluster-down:
	$(SCRIPTS_BURST) --config $(CONFIG_PATH) --state-dir $(BURST_STATE_DIR) cluster-down
