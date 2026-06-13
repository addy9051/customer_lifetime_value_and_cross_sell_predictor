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


def _resolve_dbt() -> str | None:
    """Locate the dbt executable, preferring one next to the current interpreter."""
    candidate = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    if candidate.exists():
        return str(candidate)
    return shutil.which("dbt")


def main():
    # Load environment variables into os.environ
    load_dotenv()

    # Point dbt to our custom structure
    os.environ["DBT_PROFILES_DIR"] = "data/dbt"

    dbt_exe = _resolve_dbt()
    if dbt_exe is None:
        print(
            "ERROR: 'dbt' executable not found on PATH or next to the current "
            "interpreter. Install it (e.g. `pip install dbt-core dbt-snowflake`) "
            "and re-run.",
            file=sys.stderr,
        )
        sys.exit(127)

    # Build the dbt command as an argument list — no shell, so user-supplied
    # args cannot be interpreted as shell metacharacters (no injection).
    passthrough = sys.argv[1:]
    cmd = [dbt_exe, *passthrough]
    # Only inject our project dir if the caller didn't pass their own, to avoid
    # a duplicate --project-dir (dbt's behaviour with duplicates is undefined).
    if "--project-dir" not in passthrough:
        cmd += ["--project-dir", "data/dbt"]

    print(f"Executing dbt wrapper -> {' '.join(cmd)}")

    # shell=False (default). Propagate dbt's real exit code directly so dbt
    # failures fail this wrapper on every platform (the previous `os.system`
    # `>> 8` decoding silently masked non-zero exits as success on Windows).
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
