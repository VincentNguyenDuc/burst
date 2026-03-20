#!/usr/bin/env bash
set -euo pipefail

DURATION_SECONDS="${1:-30}"
PID_FILE="${2:-.burst-dev/controller.pid}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "perf profiling requires Linux. On macOS, use Instruments or samply instead."
  exit 2
fi

if ! command -v perf >/dev/null 2>&1; then
  echo "perf is not installed. Install linux perf tools first."
  exit 2
fi

if [[ ! -f "$PID_FILE" ]]; then
  echo "pid file not found: $PID_FILE"
  exit 2
fi

PID="$(cat "$PID_FILE")"

echo "Running: perf stat -d -p $PID -- sleep $DURATION_SECONDS"
perf stat -d -p "$PID" -- sleep "$DURATION_SECONDS"
