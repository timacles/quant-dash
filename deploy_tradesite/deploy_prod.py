#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
import tomllib


BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config.toml"


def _quote(value: str) -> str:
    return shlex.quote(value)


def _run(cmd: list[str], *, capture_output: bool = False, dry_run: bool = False) -> subprocess.CompletedProcess[str]:
    printable = " ".join(_quote(part) for part in cmd)
    print(f"$ {printable}")
    if dry_run:
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    return subprocess.run(
        cmd,
        check=True,
        text=True,
        capture_output=capture_output,
    )


def _run_remote(host: str, remote_cmd: str, *, dry_run: bool = False) -> None:
    _run(["ssh", host, remote_cmd], dry_run=dry_run)


def _capture(cmd: list[str]) -> str:
    result = _run(cmd, capture_output=True)
    return result.stdout.strip()


def _load_deploy_config() -> dict[str, object]:
    with CONFIG_PATH.open("rb") as handle:
        config = tomllib.load(handle)

    deploy_config = config.get("deploy", {}).get("prod")
    if not isinstance(deploy_config, dict):
        raise ValueError("Missing [deploy.prod] section in config.toml")

    required_keys = (
        "host",
        "remote_app_dir",
        "backup_dir",
        "service",
        "port",
        "backup_keep",
        "files",
    )
    missing = [key for key in required_keys if key not in deploy_config]
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Missing deploy.prod config key(s): {missing_list}")

    files = deploy_config["files"]
    if not isinstance(files, list) or not files or not all(isinstance(item, str) and item for item in files):
        raise ValueError("deploy.prod.files must be a non-empty list of relative paths")

    return deploy_config


def _ensure_clean_main(*, allow_dirty: bool) -> str:
    branch = _capture(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    if branch != "main":
        raise ValueError(f"Deploys must run from main. Current branch: {branch}")

    status = _capture(["git", "status", "--porcelain"])
    if status and not allow_dirty:
        raise ValueError("Working tree is dirty. Commit/stash changes or pass --allow-dirty.")

    return _capture(["git", "rev-parse", "HEAD"])


def _copy_path(source_root: Path, stage_root: Path, relative_path: str) -> None:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Deploy paths must stay within the repo: {relative_path}")

    source_path = source_root / relative_path
    if not source_path.exists():
        raise FileNotFoundError(f"Configured deploy path not found: {relative_path}")

    destination_path = stage_root / relative_path
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    if source_path.is_dir():
        shutil.copytree(source_path, destination_path, dirs_exist_ok=True)
        return

    shutil.copy2(source_path, destination_path)


def _stage_release(source_root: Path, file_paths: list[str]) -> Path:
    stage_root = Path(tempfile.mkdtemp(prefix="qdash_prod_"))
    for relative_path in file_paths:
        _copy_path(source_root, stage_root, relative_path)
    return stage_root


def _build_backup_command(remote_app_dir: str, backup_dir: str, backup_name: str) -> str:
    backup_path = f"{backup_dir.rstrip('/')}/{backup_name}"
    return (
        "set -euo pipefail; "
        f"app_dir={remote_app_dir}; "
        f"backup_dir={backup_dir}; "
        f"backup_path={backup_path}; "
        'mkdir -p "$backup_dir"; '
        'if [ -d "$app_dir" ] && [ -n "$(ls -A "$app_dir" 2>/dev/null)" ]; then '
        'parent_dir=$(dirname "$app_dir"); '
        'base_dir=$(basename "$app_dir"); '
        'tar -C "$parent_dir" -czf "$backup_path" "$base_dir"; '
        'fi; '
        'mkdir -p "$app_dir"'
    )


def _build_prune_command(backup_dir: str, backup_keep: int) -> str:
    return (
        "set -euo pipefail; "
        f"backup_dir={backup_dir}; "
        'mkdir -p "$backup_dir"; '
        'cd "$backup_dir"; '
        "backups=$(ls -1t qdash_*.tar.gz 2>/dev/null || true); "
        f"if [ -n \"$backups\" ]; then printf '%s\\n' \"$backups\" | tail -n +{backup_keep + 1} | xargs -r rm -f; fi"
    )


def _build_restart_command(service: str) -> str:
    service_q = _quote(service)
    return (
        "set -euo pipefail; "
        f"systemctl restart {service_q}; "
        f"systemctl is-active --quiet {service_q}"
    )


def _build_health_check_command(port: int) -> str:
    return (
        "set -euo pipefail; "
        "python3 - <<'PY'\n"
        "from urllib.request import urlopen\n"
        f"with urlopen('http://127.0.0.1:{port}/', timeout=5) as response:\n"
        "    if response.status != 200:\n"
        "        raise SystemExit(f'Unexpected status: {response.status}')\n"
        "PY"
    )


def deploy(*, allow_dirty: bool, backup_keep_override: int | None, dry_run: bool) -> int:
    deploy_config = _load_deploy_config()
    commit_sha = _ensure_clean_main(allow_dirty=allow_dirty)

    host = str(deploy_config["host"])
    remote_app_dir = str(deploy_config["remote_app_dir"])
    backup_dir = str(deploy_config["backup_dir"])
    service = str(deploy_config["service"])
    port = int(deploy_config["port"])
    configured_backup_keep = int(deploy_config["backup_keep"])
    backup_keep = backup_keep_override if backup_keep_override is not None else configured_backup_keep
    files = [str(path) for path in deploy_config["files"]]

    if backup_keep < 1:
        raise ValueError("backup_keep must be at least 1")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"qdash_{timestamp}.tar.gz"
    backup_path = f"{backup_dir.rstrip('/')}/{backup_name}"

    print(f"Commit: {commit_sha}")
    print(f"Host: {host}")
    print(f"Remote app dir: {remote_app_dir}")
    print(f"Backup path: {backup_path}")
    print(f"Service: {service}")
    print("Deploy files:")
    for relative_path in files:
        print(f"  - {relative_path}")

    stage_root = _stage_release(BASE_DIR, files)
    print(f"Staged release: {stage_root}")

    try:
        _run_remote(host, _build_backup_command(remote_app_dir, backup_dir, backup_name), dry_run=dry_run)
        _run(
            ["rsync", "-a", f"{stage_root}/", f"{host}:{remote_app_dir}/"],
            dry_run=dry_run,
        )
        _run_remote(host, _build_prune_command(backup_dir, backup_keep), dry_run=dry_run)
        _run_remote(host, _build_restart_command(service), dry_run=dry_run)
        _run_remote(host, _build_health_check_command(port), dry_run=dry_run)
    finally:
        shutil.rmtree(stage_root, ignore_errors=True)

    print("Deploy complete.")
    print(f"Backup: {backup_path}")
    print(f"Verify: http://{host}:{port}/")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Deploy qDash to PROD using config.toml defaults.")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without modifying the remote host.")
    parser.add_argument(
        "--backup-keep",
        type=int,
        default=None,
        help="Override deploy.prod.backup_keep from config.toml.",
    )
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Allow deployment from a dirty working tree.",
    )
    args = parser.parse_args()
    return deploy(
        allow_dirty=args.allow_dirty,
        backup_keep_override=args.backup_keep,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, subprocess.CalledProcessError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
