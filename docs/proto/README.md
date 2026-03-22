# Proto documentation

This directory contains generated API docs for the `burst.v1` protobuf contracts used by controller, workers, and CLI.

## Source contracts

- `burst-core/proto/burst/v1/control.proto`
- `burst-core/proto/burst/v1/job.proto`
- `burst-core/proto/burst/v1/worker.proto`
- `burst-core/proto/burst/v1/peer.proto`

## Regenerate docs

Prerequisites:

- `protoc`
- `protoc-gen-doc` (`go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc@latest`)

From repository root:

```bash
make docs
```

Generated output:

- `docs/proto/burst.v1.md`
