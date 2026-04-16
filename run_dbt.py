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
import sys
from dotenv import load_dotenv

def main():
    # Load environment variables into os.environ
    load_dotenv()
    
    # Point dbt to our custom structure
    os.environ['DBT_PROFILES_DIR'] = 'data/dbt'
    
    # Reconstruct the dbt command passing through any UI args
    dbt_args = " ".join(sys.argv[1:])
    cmd = f"poetry run dbt {dbt_args} --project-dir data/dbt"
    
    print(f"Executing secure wrapper -> {cmd}")
    
    exit_code = os.system(cmd)
    
    # Ensure terminal returns failure if dbt fails
    sys.exit(exit_code >> 8)

if __name__ == "__main__":
    main()
