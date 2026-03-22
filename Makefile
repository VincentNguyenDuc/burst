.PHONY: build up down logs bench test steal-test docs

DOCKER_COMPOSE ?= docker compose
WORKER_COUNT ?= 8
WORKER_NAME_PREFIX ?= worker-

build:
	$(DOCKER_COMPOSE) build

up:
	@set -e; \
	$(DOCKER_COMPOSE) up -d --build controller; \
	for i in $$(seq 1 $(WORKER_COUNT)); do \
		docker rm -f $(WORKER_NAME_PREFIX)$$i >/dev/null 2>&1 || true; \
		COMPOSE_IGNORE_ORPHANS=1 $(DOCKER_COMPOSE) run \
			-d \
			--name $(WORKER_NAME_PREFIX)$$i worker \
			--config /app/burst-config/burst.config.json \
			--worker-id $(WORKER_NAME_PREFIX)$$i >/dev/null; \
	done

down:
	$(DOCKER_COMPOSE) down --remove-orphans

logs:
	@set -e; \
	containers=$$(docker ps --filter label=com.docker.compose.project=burst --format '{{.Names}}' | sort); \
	if [ -z "$$containers" ]; then \
		echo "No running burst containers"; \
		exit 0; \
	fi; \
	echo "Streaming logs from:"; \
	echo "$$containers"; \
	echo "$$containers" | xargs -I{} -P 16 sh -c 'docker logs "{}" 2>&1 | sed "s/^/[{}] /"'

test:
	$(DOCKER_COMPOSE) build test
	$(DOCKER_COMPOSE) run --rm test

format:
	cargo fmt --all
	black scripts/

docs:
	cargo doc --workspace --no-deps --quiet
	mkdir -p docs/proto
	protoc \
		-I burst-core/proto \
		--doc_out=docs/proto \
		--doc_opt=markdown,burst.v1.md \
		burst-core/proto/burst/v1/control.proto \
		burst-core/proto/burst/v1/job.proto \
		burst-core/proto/burst/v1/worker.proto \
		burst-core/proto/burst/v1/peer.proto
