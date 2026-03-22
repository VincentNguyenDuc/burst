#!/usr/bin/env python3

import argparse
import asyncio
import random
from typing import List, Optional, Sequence, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit a batch of variable-duration jobs for manual work-stealing observation"
    )
    parser.add_argument("--config", default="./burst-config/burst.config.json")
    parser.add_argument(
        "--cli-cmd",
        nargs="+",
        default=["target/release/burst-cli"],
        help=(
            "Base CLI command prefix used to invoke burst-cli from host. "
            "Default: target/release/burst-cli"
        ),
    )
    parser.add_argument("--output-dir", default="/tmp/burst-steal-out")
    parser.add_argument(
        "--job-sleeps-sec",
        type=float,
        nargs="+",
        default=[8.0, 4.0, 2.0, 1.0, 0.5],
        help="Sleep durations pool; used directly unless --job-count is provided",
    )
    parser.add_argument(
        "--job-count",
        type=int,
        default=None,
        help="Number of jobs to submit; when set, sleeps are sampled randomly from --job-sleeps-sec",
    )
    return parser.parse_args()


async def run_cli(command: Sequence[str]) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode or 0, stdout.decode().strip(), stderr.decode().strip()


def parse_job_id(stdout: str) -> Optional[str]:
    for line in stdout.splitlines():
        value = line.strip()
        if value.startswith("job-"):
            return value
    return None


async def submit_process_job(
    args: argparse.Namespace,
    sleep_seconds: float,
) -> str:
    code, stdout, stderr = await run_cli(
        [
            *args.cli_cmd,
            "--config",
            args.config,
            "submit",
            "--output-dir",
            args.output_dir,
            "process",
            "/bin/bash",
            "-lc",
            f'echo "$HOSTNAME"; sleep {max(sleep_seconds, 0.0)}',
        ]
    )
    job_id = parse_job_id(stdout)
    if code != 0 or job_id is None:
        raise RuntimeError(f"submit failed: stdout='{stdout}' stderr='{stderr}'")
    return job_id


async def main() -> int:
    args = parse_args()
    if args.job_count is not None and args.job_count < 1:
        raise RuntimeError("--job-count must be >= 1")
    if not args.job_sleeps_sec:
        raise RuntimeError("--job-sleeps-sec must contain at least one value")

    sleep_schedule = (
        [random.choice(args.job_sleeps_sec) for _ in range(args.job_count)]
        if args.job_count is not None
        else args.job_sleeps_sec
    )

    submitted_job_ids: List[str] = []
    for sleep_seconds in sleep_schedule:
        job_id = await submit_process_job(args, sleep_seconds)
        submitted_job_ids.append(job_id)

    print("submitted_jobs_total=" + str(len(submitted_job_ids)))
    print(
        "submitted_job_sleeps_sec=" + ",".join(str(value) for value in sleep_schedule)
    )
    print("submitted_job_ids=" + ",".join(submitted_job_ids))
    print("output_dir=" + args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
