#!/usr/bin/env python
"""
DBT Execution Wrapper
Automatically parses local .env securely into execution context
and sets the dbt profiles directory.

Usage:
    python run_dbt.py run
    python run_dbt.py test
    python run_dbt.py run --models stg_bookings
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv


def _resolve_dbt() -> str:
    """
    Locate the dbt executable, preferring a copy located next to the current Python interpreter.
    
    Returns:
        The path to the dbt executable to invoke. If a dbt binary exists alongside the current Python executable it is returned; otherwise the first 'dbt' found on PATH is returned, or the string 'dbt' as a final fallback.
    """
    candidate = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    if candidate.exists():
        return str(candidate)
    return shutil.which("dbt") or "dbt"


def main():
    # Load environment variables into os.environ
    """
    Prepare environment and invoke the dbt CLI, then exit with dbt's return code.
    
    Loads environment variables from a local `.env`, sets DBT_PROFILES_DIR to "data/dbt", resolves the dbt executable, builds a safe argument-list invocation including any wrapper CLI arguments and "--project-dir data/dbt", prints the command, runs dbt without a shell, and exits the process with dbt's exit status.
    """
    load_dotenv()

    # Point dbt to our custom structure
    os.environ["DBT_PROFILES_DIR"] = "data/dbt"

    # Build the dbt command as an argument list — no shell, so user-supplied
    # args cannot be interpreted as shell metacharacters (no injection).
    cmd = [_resolve_dbt(), *sys.argv[1:], "--project-dir", "data/dbt"]

    print(f"Executing dbt wrapper -> {' '.join(cmd)}")

    # shell=False (default). Propagate dbt's real exit code directly so dbt
    # failures fail this wrapper on every platform (the previous `os.system`
    # `>> 8` decoding silently masked non-zero exits as success on Windows).
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
