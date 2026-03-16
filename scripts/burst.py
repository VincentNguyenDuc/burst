#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import shlex
import sys
import time
from pathlib import Path

from util import (
    controller_addr_from_config,
    controller_bind_from_config,
    default_config_path,
    default_state_dir,
    is_pid_alive,
    kill_pid_file,
    repo_root,
    run_checked,
    start_background,
    workers_from_config,
)


def cmd_controller(args: argparse.Namespace) -> None:
    run_checked(
        ["cargo", "run", "-p", "burst-controller", "--", "--config", args.config],
        cwd=repo_root(),
    )


def cmd_submit(args: argparse.Namespace) -> None:
    argv: list[str]
    if args.argv:
        argv = list(args.argv)
    else:
        cmd_str = args.cmd or os.environ.get("CMD", "/bin/echo hello-from-burst")
        argv = shlex.split(cmd_str)

    if not argv:
        raise SystemExit("submit requires a command")

    run_checked(
        [
            "cargo",
            "run",
            "-p",
            "burst-cli",
            "--",
            "--config",
            args.config,
            "submit",
            "--output-dir",
            "./.burst-dev",
            *argv,
        ],
        cwd=repo_root(),
    )


def cmd_status(args: argparse.Namespace) -> None:
    job_id = args.job_id or os.environ.get("JOB_ID")
    if not job_id:
        raise SystemExit("status requires --job-id (or JOB_ID env var)")

    run_checked(
        [
            "cargo",
            "run",
            "-p",
            "burst-cli",
            "--",
            "--config",
            args.config,
            "status",
            "--job-id",
            job_id,
        ],
        cwd=repo_root(),
    )


def cmd_cluster_up(args: argparse.Namespace) -> None:
    state_dir = Path(args.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)

    bind_addr = controller_bind_from_config(args.config)
    print(f"Starting controller on {bind_addr}")

    controller_pid = start_background(
        ["cargo", "run", "-p", "burst-controller", "--", "--config", args.config],
        cwd=repo_root(),
        log_path=state_dir / "controller.log",
        pid_path=state_dir / "controller.pid",
    )

    time.sleep(1)

    workers = workers_from_config(args.config)
    for w in workers:
        worker_id = str(w.get("worker_id", ""))
        if not worker_id:
            raise SystemExit("workers[] entry missing worker_id")

        slots = int(w.get("slots", 1) or 1)
        print(f"Starting {worker_id} with slots={max(slots, 1)}")

        start_background(
            [
                "cargo",
                "run",
                "-p",
                "burst-worker",
                "--",
                "--config",
                args.config,
                "--worker-id",
                worker_id,
            ],
            cwd=repo_root(),
            log_path=state_dir / f"{worker_id}.log",
            pid_path=state_dir / f"{worker_id}.pid",
        )

    print("Cluster started. Use 'python3 scripts/burst.py cluster-status' to inspect.")
    _ = controller_pid


def cmd_cluster_status(args: argparse.Namespace) -> None:
    state_dir = Path(args.state_dir)

    controller_pid_file = state_dir / "controller.pid"
    print("Controller PID:")
    if controller_pid_file.exists():
        pid_text = controller_pid_file.read_text(encoding="utf-8").strip()
        try:
            pid = int(pid_text)
        except ValueError:
            print("invalid pid file")
        else:
            status = "running" if is_pid_alive(pid) else "not running"
            print(f"{pid} ({status})")
    else:
        print("not running")

    print("Workers:")
    pid_files = sorted(state_dir.glob("worker-*.pid"))
    if not pid_files:
        print("none")
        return

    for pidf in pid_files:
        pid_text = pidf.read_text(encoding="utf-8").strip()
        name = pidf.stem
        try:
            pid = int(pid_text)
        except ValueError:
            print(f"{name}: invalid pid file")
            continue
        status = "running" if is_pid_alive(pid) else "not running"
        print(f"{name}: {pid} ({status})")


def cmd_cluster_down(args: argparse.Namespace) -> None:
    state_dir = Path(args.state_dir)
    print("Stopping cluster processes...")

    kill_pid_file(state_dir / "controller.pid")
    for pidf in state_dir.glob("worker-*.pid"):
        kill_pid_file(pidf)

    print(f"Cluster stopped. Logs remain in {state_dir}/")


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--config",
        default=default_config_path(),
        help="Path to burst.config.json (or CONFIG_PATH env var)",
    )
    common.add_argument(
        "--state-dir",
        default=default_state_dir(),
        help="State dir for cluster pids/logs (or BURST_STATE_DIR env var)",
    )

    p = argparse.ArgumentParser(prog="burst", parents=[common])

    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser(
        "controller", help="Run controller in foreground", parents=[common]
    )
    sp.set_defaults(func=cmd_controller)

    sp = sub.add_parser("submit", help="Submit a job", parents=[common])
    sp.add_argument(
        "--controller",
        default=None,
        help="Override controller addr (defaults to config cli.controller_addr)",
    )
    sp.add_argument(
        "--cmd",
        default=None,
        help="Command string (defaults to CMD env var or a simple echo)",
    )
    sp.add_argument(
        "argv",
        nargs=argparse.REMAINDER,
        help="Command argv; pass after '--' (preferred)",
    )
    sp.set_defaults(func=cmd_submit)

    sp = sub.add_parser("status", help="Get job status", parents=[common])
    sp.add_argument(
        "--controller",
        default=None,
        help="Override controller addr (defaults to config cli.controller_addr)",
    )
    sp.add_argument("--job-id", default=None, help="Job id (or JOB_ID env var)")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser(
        "cluster-up", help="Start controller + all configured workers", parents=[common]
    )
    sp.set_defaults(func=cmd_cluster_up)

    sp = sub.add_parser(
        "cluster-status", help="Print cluster process status", parents=[common]
    )
    sp.set_defaults(func=cmd_cluster_status)

    sp = sub.add_parser("cluster-down", help="Stop cluster processes", parents=[common])
    sp.set_defaults(func=cmd_cluster_down)

    return p


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
