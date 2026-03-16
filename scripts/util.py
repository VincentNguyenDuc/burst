from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Iterable, Sequence


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def default_config_path() -> str:
    return os.environ.get("CONFIG_PATH", "burst.config.json")


def default_state_dir() -> str:
    return os.environ.get("BURST_STATE_DIR", ".burst-dev")


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SystemExit(f"Config not found: {p}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid JSON in {p}: {e}")


def controller_addr_from_config(config_path: str | Path) -> str:
    data = load_json(config_path)
    try:
        return str(data["cli"]["controller_addr"])
    except Exception:
        raise SystemExit("Missing config field: cli.controller_addr")


def controller_bind_from_config(config_path: str | Path) -> str:
    data = load_json(config_path)
    try:
        return str(data["controller"]["bind_addr"])
    except Exception:
        raise SystemExit("Missing config field: controller.bind_addr")


def workers_from_config(config_path: str | Path) -> list[dict[str, Any]]:
    data = load_json(config_path)
    workers = data.get("workers")
    if not isinstance(workers, list):
        raise SystemExit("Missing/invalid config field: workers (expected array)")
    return [w for w in workers if isinstance(w, dict)]


def run_checked(argv: Sequence[str], *, cwd: Path | None = None) -> None:
    try:
        subprocess.run(argv, cwd=cwd, check=True)
    except FileNotFoundError:
        raise SystemExit(f"Command not found: {argv[0]}")
    except subprocess.CalledProcessError as e:
        raise SystemExit(e.returncode)


@dataclass(frozen=True)
class ProcFiles:
    pid_file: Path
    log_file: Path


def is_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    else:
        return True


def start_background(
    argv: Sequence[str],
    *,
    log_path: Path,
    pid_path: Path,
    cwd: Path | None = None,
) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.parent.mkdir(parents=True, exist_ok=True)

    log_fp: IO[str] = log_path.open("w", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            list(argv),
            cwd=cwd,
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception:
        log_fp.close()
        raise

    pid_path.write_text(str(proc.pid), encoding="utf-8")
    return proc.pid


def kill_pid_file(pid_path: Path) -> None:
    if not pid_path.exists():
        return

    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except Exception:
        pid_path.unlink(missing_ok=True)
        return

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except PermissionError:
        pass
    finally:
        pid_path.unlink(missing_ok=True)


def print_err(msg: str) -> None:
    print(msg, file=sys.stderr)
