# burst

`burst` is a high-throughput job scheduler prototype for short process jobs running on a shared cluster.

Current POC scope:

- 1 controller process
- N worker processes
- 1 CLI client session
- gRPC communication between all components
- In-memory round-robin scheduling

Configuration is centralized in `burst.config.json` at the repository root.

## Workspace layout

- `burst-core`: shared protobuf contract and generated gRPC types
- `burst-controller`: scheduler + controller gRPC server
- `burst-worker`: worker runtime that polls, executes, and reports
- `burst-cli`: client for job submit and status
- `scripts/bench_throughput.py`: throughput benchmark runner using `burst-cli`

## Architecture

### Control plane

The controller owns in-memory state:

- pending jobs queue
- worker registry and available slots
- per-worker leased job queues
- job status map

Workers register with `worker_id` and `slots`, then repeatedly poll for work.

### Scheduling

Scheduling is pluggable through a strategy trait and registry in the controller.

- default strategy: roundrobin
- strategy selected by `controller.scheduler` in `burst.config.json`

## Shared configuration

All components load the same JSON file (`burst.config.json`) so runtime settings are tracked in one place.

Top-level sections:

- `controller`: bind address, scheduler strategy, and submission buffer capacity
- `worker`: controller address, default slots, poll/retry timing
- `cli`: controller address used by submit/status commands
- `cluster`: local test-cluster worker count and worker slots for Makefile automation

### Job lifecycle

1. CLI submits `JobSpec { command, args }`
2. Controller stores job as `queued`
3. Scheduler leases job to an available worker (`leased`)
4. Worker executes process with `tokio::process::Command`
5. Worker reports exit result
6. Controller marks job as `succeeded` or `failed`

## RPC contract

Service: `ControllerRpc`

- `SubmitJob`
- `GetJobStatus`
- `RegisterWorker`
- `PollJob`
- `ReportJobResult`
- `Heartbeat`

Proto file: `burst-core/proto/burst/v1/control.proto`

## Running locally

### Recommended: Docker Compose (cross-platform)

Use Docker Compose to avoid host environment differences between macOS and Linux.

Build runtime images:

```bash
make docker-build
```

Start controller + workers in containers:

```bash
make docker-up
```

Run throughput benchmark in containerized environment:

```bash
make docker-bench
```

Stop containers:

```bash
make docker-down
```

Tail cluster logs:

```bash
make docker-logs
```

### Throughput benchmark (`jobs/s`)

The repository includes a Python benchmark runner that submits many process jobs and waits for terminal state (`succeeded` / `failed`) before computing throughput.

Run throughput benchmark in Docker:

```bash
make docker-bench
```

Notes:

- `docker-bench` uses the benchmark command and parameters defined in `docker-compose.yml`.
- Runtime command path differences are handled inside the container.

Output includes:

- `submit_throughput_jobs_per_sec` (acceptance rate)
- `throughput_jobs_per_sec` (end-to-end completion rate)

Stop benchmark cluster (if still running):

```bash
make docker-down
```

## Tracing logs

Controller and worker emit structured lifecycle logs for:

- job submit validation and queueing
- scheduler lease decisions (`job_id`, `worker_id`, `job_type`)
- worker poll assignment and execution start/finish
- result reporting and state transitions

Default log level is `info`. Override with `RUST_LOG`.

Examples:

```bash
RUST_LOG=info cargo run -p burst-controller -- --config burst.config.json
RUST_LOG=debug cargo run -p burst-worker -- --config burst.config.json --worker-id worker-1
```

## Build and docs

Workspace check:

```bash
cargo check
```

Generate all docs (Rust + proto):

```bash
make docs
```

### Rust docs (`cargo doc`)

Generate Rust API docs for all crates:

```bash
cargo doc --workspace --no-deps
```

For private modules and internal architecture docs:

```bash
cargo doc --workspace --no-deps --document-private-items
```

Shortcuts:

```bash
make docs-rust
make docs-rust-private
```

Output location:

- `target/doc/index.html`

### Proto docs

Generate protobuf API docs from:

- `burst-core/proto/burst/v1/control.proto`
- `burst-core/proto/burst/v1/job.proto`
- `burst-core/proto/burst/v1/worker.proto`

```bash
make docs-proto
```

Output location:

- `docs/proto/burst.v1.md`

Requirements for `make docs-proto`:

- `protoc`
- `protoc-gen-doc`

## Current limitations

- single controller instance (not horizontally scaled)
- in-memory state only (no persistence)
- no retries or requeue on worker loss yet
- no authn/authz between components
- worker polling model is simple unary RPC loop