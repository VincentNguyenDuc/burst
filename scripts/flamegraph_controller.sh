#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-burst-bench.config.json}"
OUTPUT_PATH="${2:-controller-flamegraph.svg}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "cargo-flamegraph uses Linux perf. On macOS, use Instruments or samply for flamegraphs."
  exit 2
fi

if ! command -v cargo-flamegraph >/dev/null 2>&1; then
  echo "cargo-flamegraph not found. Install with: cargo install flamegraph"
  exit 2
fi

echo "Starting burst-controller under flamegraph profiling."
echo "Drive load from another terminal (e.g. make bench-throughput), then stop with Ctrl+C."
cargo flamegraph -p burst-controller --output "$OUTPUT_PATH" -- --config "$CONFIG_PATH"

echo "Flamegraph written to: $OUTPUT_PATH"
