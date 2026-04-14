"""
Snowflake Data Loader
=====================
Uploads synthetic CSV data into Snowflake RAW schema using PUT + COPY INTO.

Usage:
    python -m data.snowflake_loader --data-dir data/synthetic
"""

import argparse
import logging
import os
from pathlib import Path

import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Table → CSV filename mapping
TABLE_MAP = {
    "CORPORATE_ACCOUNTS": "corporate_accounts.csv",
    "SERVICE_CONTRACTS": "service_contracts.csv",
    "TRAVELER_PROFILES": "traveler_profiles.csv",
    "BOOKINGS": "bookings.csv",
    "SUPPORT_TICKETS": "support_tickets.csv",
}


def get_connection():
    """Create a Snowflake connection from environment variables."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "CLV_CROSS_SELL"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


def load_table(cursor, data_dir: Path, schema: str, table_name: str, csv_filename: str):
    """Upload a single CSV to Snowflake via internal stage."""
    csv_path = data_dir / csv_filename
    if not csv_path.exists():
        logger.warning("Skipping %s — file not found: %s", table_name, csv_path)
        return

    stage_name = f"@{schema}.%{table_name}"
    full_table = f"{schema}.{table_name}"

    logger.info("Loading %s → %s", csv_filename, full_table)

    # Create a file format for CSVs with headers
    cursor.execute(f"""
        CREATE OR REPLACE FILE FORMAT {schema}.CSV_FORMAT
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('', 'None', 'NaT')
            EMPTY_FIELD_AS_NULL = TRUE
    """)

    # PUT file to internal stage
    put_sql = f"PUT 'file://{csv_path.as_posix()}' {stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    cursor.execute(put_sql)
    logger.info("  → PUT complete")

    # Truncate before load (idempotent)
    cursor.execute(f"TRUNCATE TABLE IF EXISTS {full_table}")

    # COPY INTO
    cursor.execute(f"""
        COPY INTO {full_table}
        FROM {stage_name}
        FILE_FORMAT = (FORMAT_NAME = '{schema}.CSV_FORMAT')
        ON_ERROR = 'CONTINUE'
    """)

    # Verify row count
    cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
    count = cursor.fetchone()[0]
    logger.info("  → Loaded %d rows into %s", count, full_table)


def run_staging_transform(cursor):
    """Execute the staging transformation queries from the DDL file.

    This re-creates the STAGING tables from RAW data with derived columns.
    """
    logger.info("Running staging transformations...")

    ddl_path = Path(__file__).parent / "schema" / "snowflake_ddl.sql"
    if not ddl_path.exists():
        logger.warning("DDL file not found at %s — skipping staging transforms", ddl_path)
        return

    ddl_content = ddl_path.read_text()

    # Extract and run only STAGING CREATE OR REPLACE TABLE statements
    in_staging = False
    staging_statements = []
    current_stmt = []

    for line in ddl_content.split("\n"):
        if "STAGING Schema" in line:
            in_staging = True
            continue
        if "FEATURES Schema" in line:
            in_staging = False
            break
        if in_staging:
            current_stmt.append(line)
            if line.strip().endswith(";"):
                stmt = "\n".join(current_stmt).strip()
                if stmt and not stmt.startswith("--"):
                    staging_statements.append(stmt)
                current_stmt = []

    cursor.execute("CREATE SCHEMA IF NOT EXISTS STAGING")
    for stmt in staging_statements:
        if stmt.strip():
            logger.info("  → Executing staging SQL: %s...", stmt[:80])
            cursor.execute(stmt)

    logger.info("Staging transformations complete.")


def load_clv_labels(cursor, data_dir: Path):
    """Load pre-computed CLV labels into FEATURES schema."""
    csv_path = data_dir / "clv_labels.csv"
    if not csv_path.exists():
        logger.warning("CLV labels not found — skipping")
        return

    cursor.execute("CREATE SCHEMA IF NOT EXISTS FEATURES")

    stage_name = "@FEATURES.%CLV_LABELS"
    cursor.execute(f"""
        CREATE OR REPLACE FILE FORMAT FEATURES.CSV_FORMAT
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('', 'None')
    """)

    cursor.execute(f"PUT 'file://{csv_path.as_posix()}' {stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cursor.execute("TRUNCATE TABLE IF EXISTS FEATURES.CLV_LABELS")
    cursor.execute(f"""
        COPY INTO FEATURES.CLV_LABELS
        FROM {stage_name}
        FILE_FORMAT = (FORMAT_NAME = 'FEATURES.CSV_FORMAT')
        ON_ERROR = 'CONTINUE'
    """)

    cursor.execute("SELECT COUNT(*) FROM FEATURES.CLV_LABELS")
    count = cursor.fetchone()[0]
    logger.info("Loaded %d CLV labels into FEATURES.CLV_LABELS", count)


def main():
    parser = argparse.ArgumentParser(description="Load synthetic data into Snowflake")
    parser.add_argument("--data-dir", type=str, default="data/synthetic", help="Directory containing CSV files")
    parser.add_argument("--skip-staging", action="store_true", help="Skip staging transformations")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    conn = get_connection()
    cursor = conn.cursor()

    try:
        logger.info("=" * 60)
        logger.info("Snowflake Data Loader — CLV & Cross-Sell Predictor")
        logger.info("=" * 60)

        # Execute DDL to create schemas and tables
        cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW")

        ddl_path = Path(__file__).parent / "schema" / "snowflake_ddl.sql"
        if ddl_path.exists():
            logger.info("Executing DDL from %s...", ddl_path)
            ddl_content = ddl_path.read_text()
            # Execute each statement individually
            for stmt in ddl_content.split(";"):
                stmt = stmt.strip()
                if stmt and not stmt.startswith("--"):
                    try:
                        cursor.execute(stmt)
                    except Exception as e:
                        # Skip non-critical DDL errors (e.g., USE statements)
                        logger.debug("DDL statement skipped: %s", str(e)[:100])

        # Load RAW data
        for table_name, csv_file in TABLE_MAP.items():
            load_table(cursor, data_dir, "RAW", table_name, csv_file)

        # Run staging transforms
        if not args.skip_staging:
            run_staging_transform(cursor)

        # Load CLV labels
        load_clv_labels(cursor, data_dir)

        logger.info("=" * 60)
        logger.info("DATA LOAD COMPLETE")
        logger.info("=" * 60)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
