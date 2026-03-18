# Proto documentation

This directory contains generated API documentation for `burst.v1` protobuf contracts.

## Generate with local `protoc` + `protoc-gen-doc`

Prerequisites:

- `protoc`
- `protoc-gen-doc` (`go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc@latest`)

From repository root:

```bash
make docs-proto
```

Output file:

- `docs/proto/burst.v1.md`
