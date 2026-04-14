#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def _resolve_source(path_str: str) -> Path:
    src = Path(path_str)
    if src.is_file():
        return src

    # If called from a parent folder, allow resolving relative to this script.
    alt = Path(__file__).resolve().parent / path_str
    if alt.is_file():
        return alt

    raise FileNotFoundError(f"Source file not found: {path_str}")


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, text=True)


def _ssh(host: str, remote_cmd: str) -> subprocess.CompletedProcess:
    return _run(["ssh", host, remote_cmd])


def deploy_file(
    source_path: str,
    remote_host: str = "csi-laptop",
    remote_dir: str = "tradesite",
    remote_port: int = 8000,
) -> bool:
    """
    Copy a local file to remote_dir on remote_host and ensure a python http server
    is running on remote_port. Returns True if the server was started, False if
    it was already running.
    """
    src = _resolve_source(source_path)

    scp_result = _run(["scp", str(src), f"{remote_host}:{remote_dir}/"])
    if scp_result.returncode != 0:
        raise RuntimeError("scp failed")

    # Check if port is already listening (lsof, ss, then pgrep fallback)
    lsof_check = (
        "command -v lsof >/dev/null 2>&1 && "
        f"lsof -iTCP:{remote_port} -sTCP:LISTEN -Pn >/dev/null 2>&1"
    )
    if _ssh(remote_host, lsof_check).returncode == 0:
        return False

    ss_check = (
        "command -v ss >/dev/null 2>&1 && "
        f"ss -ltn | grep -q ':{remote_port} '"
    )
    if _ssh(remote_host, ss_check).returncode == 0:
        return False

    pgrep_check = f"pgrep -f 'python3 -m http.server {remote_port}' >/dev/null 2>&1"
    if _ssh(remote_host, pgrep_check).returncode == 0:
        return False

    start_cmd = (
        f"nohup python3 -m http.server {remote_port} --directory '{remote_dir}' "
        f">/tmp/{remote_dir}_http.log 2>&1 & disown"
    )
    if _ssh(remote_host, start_cmd).returncode != 0:
        raise RuntimeError("failed to start remote http server")

    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Deploy a file and start remote http server.")
    parser.add_argument("source_file", help="Path to the local source file to upload")
    parser.add_argument("--host", default="csi-laptop", help="Remote host")
    parser.add_argument("--dir", dest="remote_dir", default="tradesite", help="Remote directory")
    parser.add_argument("--port", dest="remote_port", type=int, default=8000, help="Remote port")

    args = parser.parse_args()

    started = deploy_file(
        args.source_file,
        remote_host=args.host,
        remote_dir=args.remote_dir,
        remote_port=args.remote_port,
    )

    if started:
        print(f"Started server on {args.host}:{args.remote_port}")
    else:
        print(f"Server already running on {args.host}:{args.remote_port}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
