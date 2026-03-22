# burst

`burst` is a distributed job scheduling prototype for short-lived process workloads.

## Current architecture

- Control plane: one controller process exposes gRPC APIs for submit, status, worker registration, polling, and result reporting.
- Data plane: workers execute jobs locally, maintain local leased queues, and report terminal results.
- Worker-to-worker balancing: idle workers can steal queued jobs from peer workers over gRPC.
- Scheduling: pluggable router strategies selected by `controller.router` in config.
- State model: controller holds in-memory scheduling and job lifecycle state.

## Capability summary

- Submit and track process, Python, and Docker job specs through `burst-cli`.
- Lease jobs to workers using router strategies (`roundrobin`, `power2`, `biased`).
- Queue-capacity-aware leasing: workers register slots and queue capacity; controller schedules based on available queue room.
- Peer work stealing for imbalance recovery (`StealJobs` RPC).
- Local output capture per job (`<job_id>.stdout`, `<job_id>.stderr`).
- Docker Compose workflow for controller and multi-worker clusters.

## Core components

- `burst-controller`: scheduling, leasing, status transitions, worker registry.
- `burst-worker`: polling loop, local queue execution, peer steal server/client.
- `burst-cli`: submit and status client.
- `burst-core`: shared config model, protobuf contracts, generated gRPC types.
- `scripts/test_work_stealing.py`: local batch submit utility for steal-heavy experiments.

## Job lifecycle

1. Client submits `JobSpec`.
2. Controller enqueues and leases jobs to workers according to router strategy.
3. Worker polls, enqueues locally, executes up to slot limit, and reports result.
4. Idle workers may steal queued jobs from peers.
5. Controller updates terminal state (`succeeded` or `failed`).

## RPC surfaces

- `ControllerRpc` (control plane):
  - `SubmitJob`
  - `GetJobStatus`
  - `RegisterWorker`
  - `PollJob`
  - `ReportJobResult`
  - `Heartbeat`
- `WorkerPeerRpc` (peer balancing):
  - `StealJobs`

Proto references:

- `burst-core/proto/burst/v1/control.proto`
- `burst-core/proto/burst/v1/job.proto`
- `burst-core/proto/burst/v1/worker.proto`
- `burst-core/proto/burst/v1/peer.proto`

## Running with Docker Compose

Build images:

```bash
make build
```

Start controller and workers:

```bash
make up
```

Tail logs from all running project containers:

```bash
make logs
```

Stop cluster:

```bash
make down
```

## Development commands

Run Rust tests:

```bash
cargo test -p burst-controller -p burst-worker
```

Generate Rust and proto docs:

```bash
make docs
```

## Current limitations

- Single active controller architecture; no multi-controller coordination.
- In-memory controller state; no durable job state store.
- No tenant quotas, admission control, or rate limiting.
- No authn/authz between services.
- Lease semantics are minimal and optimized for prototype speed.
