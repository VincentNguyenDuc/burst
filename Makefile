.PHONY: controller submit status build-release cluster-up cluster-up-release cluster-down cluster-status bench-throughput bench-release perf-controller flamegraph-controller docs docs-rust docs-rust-private docs-proto clean

CMD ?= /bin/echo hello-from-burst
ARGV ?=
JOB_ID ?=

BURST_STATE_DIR ?= .burst-dev
CONFIG_PATH ?= burst-example.config.json
OUTPUT_DIR ?= ./.burst-dev/job-outputs
JOBS ?= 10000
SUBMIT_CONCURRENCY ?= 256
POLL_INTERVAL_MS ?= 5
BENCH_CMD ?= /bin/true
BENCH_ARGS ?=
PROFILE_SECONDS ?= 30
PROFILE_PID_FILE ?= $(BURST_STATE_DIR)/controller.pid
FLAMEGRAPH_OUTPUT ?= controller-flamegraph.svg
RELEASE_BIN_DIR ?= target/release

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

build-release:
	cargo build --workspace --release

cluster-up-release:
	mkdir -p "$(BURST_STATE_DIR)"
	echo "Starting release controller on $(CONTROLLER_BIND)"
	nohup "$(RELEASE_BIN_DIR)/burst-controller" --config "$(CONFIG_PATH)" >"$(BURST_STATE_DIR)/controller.log" 2>&1 & echo $$! >"$(BURST_STATE_DIR)/controller.pid"
	sleep 1
	for wid in $(WORKER_IDS); do \
		echo "Starting $$wid (release)"; \
		nohup "$(RELEASE_BIN_DIR)/burst-worker" --config "$(CONFIG_PATH)" --worker-id "$$wid" >"$(BURST_STATE_DIR)/$$wid.log" 2>&1 & echo $$! >"$(BURST_STATE_DIR)/$$wid.pid"; \
	done
	echo "Release cluster started."

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

bench-throughput:
	cargo build -p burst-cli --release
	python3 scripts/bench_throughput.py \
		--config "$(CONFIG_PATH)" \
		--jobs "$(JOBS)" \
		--submit-concurrency "$(SUBMIT_CONCURRENCY)" \
		--poll-interval-ms "$(POLL_INTERVAL_MS)" \
		--command "$(BENCH_CMD)" \
		--cli-bin "target/release/burst-cli" \
		$(foreach arg,$(BENCH_ARGS),--arg "$(arg)")

bench-release:
	@set -e; \
	$(MAKE) build-release; \
	$(MAKE) cluster-up-release CONFIG_PATH="$(CONFIG_PATH)"; \
	trap '$(MAKE) cluster-down' EXIT; \
	$(MAKE) bench-throughput \
		CONFIG_PATH="$(CONFIG_PATH)" \
		JOBS="$(JOBS)" \
		SUBMIT_CONCURRENCY="$(SUBMIT_CONCURRENCY)" \
		POLL_INTERVAL_MS="$(POLL_INTERVAL_MS)" \
		BENCH_CMD="$(BENCH_CMD)" \
		BENCH_ARGS="$(BENCH_ARGS)"; \
	trap - EXIT; \
	$(MAKE) cluster-down

perf-controller:
	bash scripts/perf_stat_pid.sh "$(PROFILE_SECONDS)" "$(PROFILE_PID_FILE)"

flamegraph-controller:
	bash scripts/flamegraph_controller.sh "$(CONFIG_PATH)" "$(FLAMEGRAPH_OUTPUT)"

clean:
	rm -rf "$(BURST_STATE_DIR)"
	cargo clean

docs: docs-rust docs-proto

docs-rust:
	cargo doc --workspace --no-deps --quiet

docs-proto:
	mkdir -p docs/proto
	protoc \
		-I burst-core/proto \
		--doc_out=docs/proto \
		--doc_opt=markdown,burst.v1.md \
		burst-core/proto/burst/v1/control.proto \
		burst-core/proto/burst/v1/job.proto \
		burst-core/proto/burst/v1/worker.proto
