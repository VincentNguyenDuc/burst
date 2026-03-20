.PHONY: build up down logs bench docs

DOCKER_COMPOSE ?= docker compose
WORKER_SERVICES ?= worker-1 worker-2 worker-3 worker-4 worker-5 worker-6 worker-7 worker-8
DOCKER_CLUSTER_SERVICES ?= controller $(WORKER_SERVICES)

build:
	$(DOCKER_COMPOSE) build

up:
	$(DOCKER_COMPOSE) up -d $(DOCKER_CLUSTER_SERVICES)

down:
	$(DOCKER_COMPOSE) down --remove-orphans

logs:
	$(DOCKER_COMPOSE) logs -f --tail=200 $(DOCKER_CLUSTER_SERVICES)

bench:
	@set -e; \
	$(MAKE) build; \
	$(MAKE) up; \
	trap '$(MAKE) down' EXIT; \
	$(DOCKER_COMPOSE) run --rm bench; \
	trap - EXIT; \
	$(MAKE) down

docs:
	cargo doc --workspace --no-deps --quiet
	mkdir -p docs/proto
	protoc \
		-I burst-core/proto \
		--doc_out=docs/proto \
		--doc_opt=markdown,burst.v1.md \
		burst-core/proto/burst/v1/control.proto \
		burst-core/proto/burst/v1/job.proto \
		burst-core/proto/burst/v1/worker.proto
