# burst

`burst` is a high-throughput job scheduler prototype for short process jobs running on a shared cluster.

Current POC scope:

- 1 controller process
- N worker processes
- 1 CLI client session
- gRPC communication between all components
- In-memory FIFO scheduling

Configuration is centralized in `burst.config.json` at the repository root.

## Workspace layout

- `burst-core`: shared protobuf contract and generated gRPC types
- `burst-controller`: scheduler + controller gRPC server
- `burst-worker`: worker runtime that polls, executes, and reports
- `burst-cli`: client for job submit and status

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

- default strategy: FIFO
- strategy selected by `controller.scheduler` in `burst.config.json`

## Shared configuration

All components load the same JSON file (`burst.config.json`) so runtime settings are tracked in one place.

Top-level sections:

- `controller`: bind address and scheduler strategy
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

### Using Makefile cluster targets

Start controller + many workers:

```bash
make cluster-up
```

By default this reads `cluster.num_workers` and `cluster.worker_slots` from `burst.config.json`.

Submit a job:

```bash
make submit CMD="/bin/echo hello-from-burst"
```

Check status:

```bash
make status JOB_ID=job-00000001
```

Inspect cluster process status:

```bash
make cluster-status
```

Stop all cluster processes:

```bash
make cluster-down
```

### Manual run

Controller:

```bash
cargo run -p burst-controller -- --config burst.config.json
```

Worker:

```bash
cargo run -p burst-worker -- --config burst.config.json --worker-id worker-1 --slots 1
```

CLI submit:

```bash
cargo run -p burst-cli -- --config burst.config.json submit /bin/echo hello
```

CLI status:

```bash
cargo run -p burst-cli -- --config burst.config.json status --job-id job-00000001
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