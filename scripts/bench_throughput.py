#!/usr/bin/env python3

import argparse
import asyncio
import random
import time
from typing import List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Throughput benchmark for burst")
    parser.add_argument("--config", default="burst-example.config.json")
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--submit-concurrency", type=int, default=128)
    parser.add_argument("--poll-interval-ms", type=int, default=10)
    parser.add_argument("--command", default="/bin/true")
    parser.add_argument("--arg", dest="args", action="append", default=[])
    parser.add_argument("--cli-bin", default="target/release/burst-cli")
    parser.add_argument("--max-submit-attempts", type=int, default=20)
    return parser.parse_args()


async def run_cli(cli_bin: str, cli_args: List[str]) -> Tuple[Optional[int], str, str]:
    proc = await asyncio.create_subprocess_exec(
        cli_bin,
        *cli_args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode, stdout.decode().strip(), stderr.decode().strip()


def parse_job_id(stdout: str) -> Optional[str]:
    for line in stdout.splitlines():
        value = line.strip()
        if value.startswith("job-"):
            return value
    return None


def parse_status(stdout: str) -> Optional[str]:
    for line in stdout.splitlines():
        parts = line.strip().split()
        if len(parts) >= 2 and parts[0].startswith("job-"):
            return parts[1]
    return None


async def submit_one(args: argparse.Namespace, semaphore: asyncio.Semaphore) -> str:
    async with semaphore:
        for attempt in range(1, args.max_submit_attempts + 1):
            code, stdout, stderr = await run_cli(
                args.cli_bin,
                [
                    "--config",
                    args.config,
                    "submit",
                    "process",
                    args.command,
                    *args.args,
                ],
            )
            job_id = parse_job_id(stdout)
            if code == 0 and job_id is not None:
                return job_id

            if attempt == args.max_submit_attempts:
                raise RuntimeError(
                    f"submit failed after {attempt} attempts: stdout='{stdout}' stderr='{stderr}'"
                )

            backoff = min(0.05 * (2 ** (attempt - 1)), 1.0)
            jitter = random.uniform(0.0, 0.03)
            await asyncio.sleep(backoff + jitter)

    raise RuntimeError("unreachable")


async def fetch_status(
    args: argparse.Namespace,
    job_id: str,
    semaphore: asyncio.Semaphore,
) -> Optional[str]:
    async with semaphore:
        code, stdout, _stderr = await run_cli(
            args.cli_bin,
            ["--config", args.config, "status", "--job-id", job_id],
        )
        if code != 0:
            return None
        return parse_status(stdout)


async def main() -> int:
    args = parse_args()
    if args.jobs <= 0:
        print("jobs must be greater than 0")
        return 2

    submit_concurrency = max(args.submit_concurrency, 1)
    poll_interval_sec = max(args.poll_interval_ms, 1) / 1000.0

    submit_semaphore = asyncio.Semaphore(submit_concurrency)
    status_semaphore = asyncio.Semaphore(submit_concurrency)

    total_start = time.perf_counter()
    submit_start = time.perf_counter()

    submit_tasks = [
        asyncio.create_task(submit_one(args, submit_semaphore))
        for _ in range(args.jobs)
    ]
    job_ids = await asyncio.gather(*submit_tasks)

    submit_elapsed = time.perf_counter() - submit_start

    pending = set(job_ids)
    succeeded = 0
    failed = 0

    while pending:
        batch = list(pending)
        tasks = [
            asyncio.create_task(fetch_status(args, job_id, status_semaphore))
            for job_id in batch
        ]
        states = await asyncio.gather(*tasks)

        for job_id, state in zip(batch, states):
            if state == "succeeded":
                pending.remove(job_id)
                succeeded += 1
            elif state == "failed":
                pending.remove(job_id)
                failed += 1

        if pending:
            await asyncio.sleep(poll_interval_sec)

    total_elapsed = time.perf_counter() - total_start

    submit_throughput = args.jobs / submit_elapsed
    throughput = args.jobs / total_elapsed

    print(f"jobs_total={args.jobs}")
    print(f"jobs_succeeded={succeeded}")
    print(f"jobs_failed={failed}")
    print(f"submit_elapsed_sec={submit_elapsed:.6f}")
    print(f"total_elapsed_sec={total_elapsed:.6f}")
    print(f"submit_throughput_jobs_per_sec={submit_throughput:.2f}")
    print(f"throughput_jobs_per_sec={throughput:.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
