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
import re
from pathlib import Path
from dotenv import load_dotenv

import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# SECURITY: Identifier allowlists to prevent SQL injection (VULN-003)
# =============================================================================
VALID_DATABASES = {"CLV_CROSS_SELL"}
VALID_SCHEMAS = {"RAW", "STAGING", "FEATURES"}
VALID_TABLES = {
    "CORPORATE_ACCOUNTS", "SERVICE_CONTRACTS", "TRAVELER_PROFILES",
    "BOOKINGS", "SUPPORT_TICKETS", "CLV_LABELS", "CSV_FORMAT",
}

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_identifier(name: str, valid_set: set | None = None) -> str:
    """Validate and double-quote a Snowflake identifier to prevent SQL injection.

    Args:
        name: The raw identifier string.
        valid_set: Optional allowlist. If provided, name must be in this set.

    Returns:
        A double-quoted, validated Snowflake identifier.

    Raises:
        ValueError: If the identifier fails validation.
    """
    stripped = name.strip().upper()
    if valid_set and stripped not in valid_set:
        raise ValueError(f"Invalid SQL identifier '{name}' — not in allowlist {valid_set}")
    if not _IDENTIFIER_RE.match(stripped):
        raise ValueError(f"Invalid SQL identifier format: '{name}'")
    return f'"{stripped}"'


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


def load_table(cursor, data_dir: Path, database: str, schema: str, table_name: str, csv_filename: str):
    """Upload a single CSV to Snowflake via internal stage."""
    csv_path = data_dir / csv_filename
    if not csv_path.exists():
        logger.warning("Skipping %s — file not found: %s", table_name, csv_path)
        return

    # SECURITY: Validate all identifiers against allowlists (VULN-003)
    db_id = safe_identifier(database, VALID_DATABASES)
    schema_id = safe_identifier(schema, VALID_SCHEMAS)
    table_id = safe_identifier(table_name, VALID_TABLES)

    stage_name = f"@{db_id}.{schema_id}.%{table_id}"
    full_table = f"{db_id}.{schema_id}.{table_id}"
    format_name = f"{db_id}.{schema_id}.{safe_identifier('CSV_FORMAT', VALID_TABLES)}"

    logger.info("Loading %s → %s", csv_filename, full_table)

    # Create a file format for CSVs with headers
    cursor.execute(f"""
        CREATE OR REPLACE FILE FORMAT {format_name}
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('', 'None', 'NaT')
            EMPTY_FIELD_AS_NULL = TRUE
    """)

    # PUT file to internal stage — path is validated via Path object (no user input)
    put_sql = f"PUT 'file://{csv_path.as_posix()}' {stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    cursor.execute(put_sql)
    logger.info("  → PUT complete")

    # Truncate before load (idempotent)
    cursor.execute(f"TRUNCATE TABLE IF EXISTS {full_table}")

    # COPY INTO
    cursor.execute(f"""
        COPY INTO {full_table}
        FROM {stage_name}
        FILE_FORMAT = (FORMAT_NAME = {format_name})
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


def load_clv_labels(cursor, data_dir: Path, database: str):
    """Load pre-computed CLV labels into FEATURES schema."""
    csv_path = data_dir / "clv_labels.csv"
    if not csv_path.exists():
        logger.warning("CLV labels not found — skipping")
        return

    # SECURITY: Validate identifiers (VULN-003)
    db_id = safe_identifier(database, VALID_DATABASES)
    features_id = safe_identifier("FEATURES", VALID_SCHEMAS)
    labels_id = safe_identifier("CLV_LABELS", VALID_TABLES)
    format_id = safe_identifier("CSV_FORMAT", VALID_TABLES)

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {db_id}.{features_id}")

    stage_name = f"@{db_id}.{features_id}.%{labels_id}"
    cursor.execute(f"""
        CREATE OR REPLACE FILE FORMAT {db_id}.{features_id}.{format_id}
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('', 'None')
    """)

    cursor.execute(f"PUT 'file://{csv_path.as_posix()}' {stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cursor.execute(f"TRUNCATE TABLE IF EXISTS {db_id}.{features_id}.{labels_id}")
    cursor.execute(f"""
        COPY INTO {db_id}.{features_id}.{labels_id}
        FROM {stage_name}
        FILE_FORMAT = (FORMAT_NAME = {db_id}.{features_id}.{format_id})
        ON_ERROR = 'CONTINUE'
    """)

    cursor.execute(f"SELECT COUNT(*) FROM {db_id}.{features_id}.{labels_id}")
    count = cursor.fetchone()[0]
    logger.info("Loaded %d CLV labels into %s.%s.%s", count, db_id, features_id, labels_id)


def main():
    # Load environment variables from .env file
    load_dotenv()

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

        # 1. Execute DDL first to create Database, Warehouse, and Schemas
        ddl_path = Path(__file__).parent / "schema" / "snowflake_ddl.sql"
        if ddl_path.exists():
            logger.info("Executing DDL from %s...", ddl_path)
            ddl_content = ddl_path.read_text()
            try:
                # Use Snowflake's native multi-statement execution for robustness
                conn.execute_string(ddl_content)
                logger.info("  → DDL execution successful")
            except Exception as e:
                logger.error("DDL execution failed. If the database already exists, this might be safe. Error: %s", str(e))
        else:
            logger.warning("DDL file NOT found at %s. Creating RAW schema manually.", ddl_path)
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.RAW")

        # 2. Load RAW data into the now-active database context
        database = os.environ.get("SNOWFLAKE_DATABASE", "CLV_CROSS_SELL")
        for table_name, csv_file in TABLE_MAP.items():
            load_table(cursor, data_dir, database, "RAW", table_name, csv_file)

        # 3. Run staging transforms (already qualified in DDL, but good to be safe)
        if not args.skip_staging:
            run_staging_transform(cursor)

        # 4. Load CLV labels
        load_clv_labels(cursor, data_dir, database)

        logger.info("=" * 60)
        logger.info("DATA LOAD COMPLETE")
        logger.info("=" * 60)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
