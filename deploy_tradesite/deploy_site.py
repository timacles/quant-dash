#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path


DEFAULT_HOST = "csi-laptop"
DEFAULT_REMOTE_ROOT = "~/PROD"
DEFAULT_SOURCE = "~/trading"
DEFAULT_BACKUP_KEEP = 5
DEFAULT_PORT = 8000

EXCLUDES = [
    ".git/",
    ".gitignore",
    ".codex",
    ".venv/",
    "__pycache__/",
    "*.pyc",
    "*.pyo",
    "*.log",
    "backups/",
    "renderer/.venv/",
    "renderer/__pycache__/",
    "renderer/*.html",
    "renderer/*.json",
    "reports/",
    "TODO.org~",
]


def _expand_path(path_str: str) -> Path:
    return Path(path_str).expanduser().resolve()


def _quote(path: str) -> str:
    return shlex.quote(path)


def _remote_path_expr(path: str) -> str:
    if path.startswith("~/"):
        return path
    return _quote(path)


def _run(cmd: list[str], *, dry_run: bool = False) -> None:
    printable = " ".join(_quote(part) for part in cmd)
    print(f"$ {printable}")
    if dry_run:
        return
    subprocess.run(cmd, check=True, text=True)


def _capture(cmd: list[str]) -> str:
    result = subprocess.run(cmd, check=True, text=True, capture_output=True)
    return result.stdout.strip()


def _stage_source(source_dir: Path, dry_run: bool) -> Path:
    stage_dir = Path(tempfile.mkdtemp(prefix="deploy_trading_"))
    rsync_cmd = ["rsync", "-a", "--delete"]
    for pattern in EXCLUDES:
        rsync_cmd.extend(["--exclude", pattern])
    rsync_cmd.extend([f"{source_dir}/", f"{stage_dir}/"])
    _run(rsync_cmd, dry_run=dry_run)
    return stage_dir


def _remote_command(host: str, command: str, *, dry_run: bool = False) -> None:
    _run(["ssh", host, command], dry_run=dry_run)


def _restart_command(remote_dir: str, port: int) -> str:
    remote_dir_q = _remote_path_expr(remote_dir)
    log_path_q = _remote_path_expr(f"{remote_dir}/serve_dashboard.log")
    process_match_q = _quote("python3 .*serve_dashboard.py")

    return (
        "set -euo pipefail; "
        f"if pgrep -f {process_match_q} >/dev/null 2>&1; then "
        f"pkill -f {process_match_q}; "
        "fi; "
        f"cd {remote_dir_q}; "
        f"nohup python3 serve_dashboard.py > {log_path_q} 2>&1 < /dev/null & "
        "disown; "
        "sleep 1; "
        f"pgrep -f {process_match_q} >/dev/null 2>&1; "
        f"python3 - <<'PY'\n"
        "import socket\n"
        f"sock = socket.create_connection(('127.0.0.1', {port}), timeout=3)\n"
        "sock.close()\n"
        "PY"
    )


def deploy(
    *,
    host: str,
    source_dir: Path,
    remote_root: str,
    dry_run: bool,
    backup_keep: int,
    port: int,
) -> None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    remote_dir = f"{remote_root}/trading"
    remote_backup_dir = f"{remote_root}/backups"
    backup_name = f"trading_{timestamp}.tar.gz"
    backup_path = f"{remote_backup_dir}/{backup_name}"

    print(f"Source: {source_dir}")
    print(f"Remote root: {remote_root}")
    print(f"Remote dir: {remote_dir}")
    print(f"Backup: {backup_path}")

    stage_dir = _stage_source(source_dir, dry_run=dry_run)
    print(f"Stage dir: {stage_dir}")

    try:
        prepare_cmd = (
            "set -euo pipefail; "
            f"mkdir -p {_remote_path_expr(remote_backup_dir)} {_remote_path_expr(remote_dir)}; "
            f"if [ -n \"$(ls -A {_remote_path_expr(remote_dir)} 2>/dev/null)\" ]; then "
            f"tar -C {_remote_path_expr(remote_root)} -czf {_remote_path_expr(backup_path)} trading; "
            "fi"
        )
        _remote_command(host, prepare_cmd, dry_run=dry_run)

        rsync_cmd = [
            "rsync",
            "-a",
            "--delete",
            f"{stage_dir}/",
            f"{host}:{remote_dir}/",
        ]
        _run(rsync_cmd, dry_run=dry_run)

        prune_cmd = (
            "set -euo pipefail; "
            f"mkdir -p {_remote_path_expr(remote_backup_dir)}; "
            f"cd {_remote_path_expr(remote_backup_dir)}; "
            "backups=$(ls -1t trading_*.tar.gz 2>/dev/null || true); "
            f"if [ -n \"$backups\" ]; then printf '%s\\n' \"$backups\" | tail -n +{backup_keep + 1} | xargs -r rm -f; fi"
        )
        _remote_command(host, prune_cmd, dry_run=dry_run)

        restart_cmd = _restart_command(remote_dir, port)
        _remote_command(host, restart_cmd, dry_run=dry_run)

        print(f"Deploy complete. Verify at http://{host}:{port}/")
    finally:
        if dry_run:
            print(f"Dry run complete. Stage directory kept at {stage_dir}")
        else:
            shutil.rmtree(stage_dir, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deploy the trading site to csi-laptop with a timestamped backup."
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="Remote SSH host.")
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="Local source directory to stage and deploy.",
    )
    parser.add_argument(
        "--remote-root",
        default=DEFAULT_REMOTE_ROOT,
        help="Remote root containing the live trading directory.",
    )
    parser.add_argument(
        "--backup-keep",
        type=int,
        default=DEFAULT_BACKUP_KEEP,
        help="Number of remote backups to retain.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="Port used by serve_dashboard.py for a post-restart health check.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the actions without modifying the remote host.",
    )

    args = parser.parse_args()
    source_dir = _expand_path(args.source)
    if not source_dir.is_dir():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")
    if args.backup_keep < 1:
        raise ValueError("--backup-keep must be at least 1")

    deploy(
        host=args.host,
        source_dir=source_dir,
        remote_root=args.remote_root,
        dry_run=args.dry_run,
        backup_keep=args.backup_keep,
        port=args.port,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, subprocess.CalledProcessError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
