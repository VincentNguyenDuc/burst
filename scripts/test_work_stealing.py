#!/usr/bin/env python3

import argparse
import asyncio
import pathlib
import time
from typing import Dict, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Integration test for worker P2P stealing"
    )
    parser.add_argument(
        "--config", default="/app/burst-config/burst-steal-test.config.json"
    )
    parser.add_argument("--cli-bin", default="/usr/local/bin/burst-cli")
    parser.add_argument("--poll-interval-ms", type=int, default=100)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--output-dir", default="/tmp/burst-steal-out")
    parser.add_argument("--long-sleep-sec", type=float, default=8.0)
    parser.add_argument("--short-sleep-sec", type=float, default=0.2)
    parser.add_argument("--short-jobs", type=int, default=3)
    parser.add_argument("--expected-busy-worker", default="steal-worker-1")
    return parser.parse_args()


async def run_cli(cli_bin: str, cli_args: List[str]) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        cli_bin,
        *cli_args,
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


def parse_status(stdout: str) -> Optional[str]:
    for line in stdout.splitlines():
        parts = line.strip().split()
        if len(parts) >= 2 and parts[0].startswith("job-"):
            return parts[1]
    return None


async def wait_for_controller(args: argparse.Namespace) -> None:
    deadline = time.monotonic() + args.timeout_sec
    while time.monotonic() < deadline:
        code, _stdout, _stderr = await run_cli(
            args.cli_bin,
            ["--config", args.config, "status", "--job-id", "job-00000000"],
        )
        if code == 0:
            return
        await asyncio.sleep(0.2)
    raise RuntimeError("controller did not become reachable before timeout")


async def submit_process_job(
    args: argparse.Namespace,
    shell_command: str,
) -> str:
    code, stdout, stderr = await run_cli(
        args.cli_bin,
        [
            "--config",
            args.config,
            "submit",
            "--output-dir",
            args.output_dir,
            "process",
            "/bin/sh",
            "-c",
            shell_command,
        ],
    )
    job_id = parse_job_id(stdout)
    if code != 0 or job_id is None:
        raise RuntimeError(f"submit failed: stdout='{stdout}' stderr='{stderr}'")
    return job_id


async def wait_for_terminal_states(
    args: argparse.Namespace, job_ids: List[str]
) -> Dict[str, str]:
    deadline = time.monotonic() + args.timeout_sec
    states: Dict[str, str] = {job_id: "unknown" for job_id in job_ids}
    pending = set(job_ids)

    while pending and time.monotonic() < deadline:
        for job_id in list(pending):
            code, stdout, stderr = await run_cli(
                args.cli_bin,
                ["--config", args.config, "status", "--job-id", job_id],
            )
            if code != 0:
                continue
            state = parse_status(stdout)
            if state is None:
                raise RuntimeError(
                    f"failed to parse state for {job_id}: stdout='{stdout}' stderr='{stderr}'"
                )

            states[job_id] = state
            if state in {"succeeded", "failed"}:
                pending.remove(job_id)

        if pending:
            await asyncio.sleep(max(args.poll_interval_ms, 1) / 1000.0)

    if pending:
        raise RuntimeError(f"timed out waiting for terminal states: {sorted(pending)}")
    return states


def read_job_hostname(output_dir: pathlib.Path, job_id: str) -> str:
    stdout_path = output_dir / f"{job_id}.stdout"
    if not stdout_path.exists():
        raise RuntimeError(f"missing stdout file for {job_id}: {stdout_path}")

    content = stdout_path.read_text(encoding="utf-8")
    first_line = content.splitlines()[0].strip() if content.splitlines() else ""
    if not first_line:
        raise RuntimeError(f"stdout for {job_id} is empty: {stdout_path}")
    return first_line


async def main() -> int:
    args = parse_args()
    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for path in output_dir.glob("job-*.stdout"):
        path.unlink(missing_ok=True)
    for path in output_dir.glob("job-*.stderr"):
        path.unlink(missing_ok=True)

    await wait_for_controller(args)

    long_job = await submit_process_job(
        args,
        f'echo "$HOSTNAME"; sleep {args.long_sleep_sec}',
    )

    short_jobs = []
    for _ in range(max(args.short_jobs, 1)):
        short_job = await submit_process_job(
            args,
            f'echo "$HOSTNAME"; sleep {args.short_sleep_sec}',
        )
        short_jobs.append(short_job)

    all_jobs = [long_job, *short_jobs]
    states = await wait_for_terminal_states(args, all_jobs)

    failed_jobs = [job_id for job_id, state in states.items() if state != "succeeded"]
    if failed_jobs:
        raise RuntimeError(f"expected all jobs succeeded, failed={failed_jobs}")

    hostnames = {job_id: read_job_hostname(output_dir, job_id) for job_id in all_jobs}

    long_job_hostname = hostnames[long_job]
    short_job_hostnames = [hostnames[job_id] for job_id in short_jobs]

    if long_job_hostname != args.expected_busy_worker:
        raise RuntimeError(
            "expected long job to stay on busy worker "
            f"'{args.expected_busy_worker}', got '{long_job_hostname}'"
        )

    stolen_short_jobs = [
        job_id
        for job_id in short_jobs
        if hostnames[job_id] != args.expected_busy_worker
    ]

    if not stolen_short_jobs:
        raise RuntimeError(
            "expected at least one short job to run on a different worker "
            f"than {args.expected_busy_worker}, got hostnames={short_job_hostnames}"
        )

    print("work_stealing_test=passed")
    print(f"long_job_id={long_job}")
    print(f"long_job_worker={long_job_hostname}")
    print(f"short_jobs_total={len(short_jobs)}")
    print(f"short_jobs_stolen={len(stolen_short_jobs)}")
    print("short_job_workers=" + ",".join(short_job_hostnames))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
