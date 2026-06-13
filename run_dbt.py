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
    """Locate the dbt executable, preferring one next to the current interpreter."""
    candidate = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    if candidate.exists():
        return str(candidate)
    return shutil.which("dbt") or "dbt"


def main():
    # Load environment variables into os.environ
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
