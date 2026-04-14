"""
Airflow DAG: Data Ingestion Pipeline
=====================================
Orchestrates the end-to-end data ingestion workflow:

    1. Generate synthetic corporate travel data
    2. Load raw CSVs into Snowflake RAW schema
    3. Transform RAW → STAGING with derived columns
    4. Load CLV labels into FEATURES schema

Schedule: Manual trigger (for development); daily in production.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# -------------------------------------------------------
# DAG Configuration
# -------------------------------------------------------

default_args = {
    "owner": "clv-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ROOT = "/opt/airflow/project"  # Mounted via docker-compose
DATA_DIR = f"{PROJECT_ROOT}/data/synthetic"

dag = DAG(
    dag_id="data_ingestion_pipeline",
    default_args=default_args,
    description="Synthesize corporate travel data → Load into Snowflake → Transform to staging",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["clv", "data-ingestion", "snowflake"],
)

# -------------------------------------------------------
# Task 1: Generate Synthetic Data
# -------------------------------------------------------

generate_data = BashOperator(
    task_id="generate_synthetic_data",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        f"python -m data.generate_synthetic_data "
        f"--output-dir {DATA_DIR} "
        f"--seed 42 "
        f"--format both"
    ),
    dag=dag,
)

# -------------------------------------------------------
# Task 2: Validate Data Quality
# -------------------------------------------------------

def _validate_data_quality(**kwargs):
    """Run basic data quality checks on generated files."""
    from pathlib import Path

    import pandas as pd

    data_dir = Path(DATA_DIR)
    issues = []

    expected_files = {
        "corporate_accounts.csv": {"min_rows": 4500, "required_cols": ["account_id", "tier", "is_churned"]},
        "bookings.csv": {"min_rows": 800_000, "required_cols": ["booking_id", "amount", "booking_date"]},
        "traveler_profiles.csv": {"min_rows": 20_000, "required_cols": ["traveler_id", "account_id"]},
        "service_contracts.csv": {"min_rows": 5_000, "required_cols": ["contract_id", "product"]},
        "support_tickets.csv": {"min_rows": 10_000, "required_cols": ["ticket_id", "severity"]},
        "clv_labels.csv": {"min_rows": 4500, "required_cols": ["account_id", "clv_12m"]},
    }

    for filename, checks in expected_files.items():
        filepath = data_dir / filename
        if not filepath.exists():
            issues.append(f"MISSING: {filename}")
            continue

        df = pd.read_csv(filepath)
        if len(df) < checks["min_rows"]:
            issues.append(f"LOW ROW COUNT: {filename} has {len(df)} rows (expected >= {checks['min_rows']})")

        missing_cols = set(checks["required_cols"]) - set(df.columns)
        if missing_cols:
            issues.append(f"MISSING COLUMNS in {filename}: {missing_cols}")

        # Check for nulls in primary key columns
        pk_col = checks["required_cols"][0]
        null_pks = df[pk_col].isnull().sum()
        if null_pks > 0:
            issues.append(f"NULL PKs in {filename}.{pk_col}: {null_pks}")

    if issues:
        raise ValueError("Data quality issues found:\n" + "\n".join(f"  - {i}" for i in issues))

    print("✅ All data quality checks passed!")


validate_data = PythonOperator(
    task_id="validate_data_quality",
    python_callable=_validate_data_quality,
    dag=dag,
)

# -------------------------------------------------------
# Task 3: Load into Snowflake RAW
# -------------------------------------------------------

load_snowflake = BashOperator(
    task_id="load_snowflake_raw",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        f"python -m data.snowflake_loader "
        f"--data-dir {DATA_DIR}"
    ),
    dag=dag,
)

# -------------------------------------------------------
# Task 4: Log Summary Metrics
# -------------------------------------------------------

def _log_summary(**kwargs):
    """Log summary statistics to Airflow XCom for downstream monitoring."""
    from pathlib import Path

    import pandas as pd

    data_dir = Path(DATA_DIR)

    summary = {}
    for csv_file in data_dir.glob("*.csv"):
        df = pd.read_csv(csv_file)
        summary[csv_file.stem] = {
            "rows": len(df),
            "columns": len(df.columns),
            "size_mb": round(csv_file.stat().st_size / (1024 * 1024), 2),
        }

    print("=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    for name, stats in summary.items():
        print(f"  {name:30s} → {stats['rows']:>10,} rows | {stats['columns']:>3} cols | {stats['size_mb']:.1f} MB")
    print("=" * 60)

    # Push to XCom for downstream tasks
    kwargs["ti"].xcom_push(key="pipeline_summary", value=summary)


log_summary = PythonOperator(
    task_id="log_pipeline_summary",
    python_callable=_log_summary,
    dag=dag,
)

# -------------------------------------------------------
# Task Dependencies
# -------------------------------------------------------
# generate → validate → load → summary

generate_data >> validate_data >> load_snowflake >> log_summary
