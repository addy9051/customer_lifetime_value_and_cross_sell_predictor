"""
Airflow DAG: Feature Engineering Pipeline
===========================================
Orchestrates the feature computation workflow:

    1. Run feature engineering on the latest synthetic data
    2. Validate computed features
    3. (Optional) Upload features to Snowflake FEATURES schema

Schedule: Runs after data_ingestion_pipeline completes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "clv-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ROOT = "/opt/airflow/project"

dag = DAG(
    dag_id="feature_engineering_pipeline",
    default_args=default_args,
    description="Compute ML features from raw data → write to feature store",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["clv", "feature-engineering", "ml"],
)

# -------------------------------------------------------
# Task 1: Wait for data ingestion (optional sensor)
# -------------------------------------------------------

# wait_for_data = ExternalTaskSensor(
#     task_id="wait_for_data_ingestion",
#     external_dag_id="data_ingestion_pipeline",
#     external_task_id="log_pipeline_summary",
#     timeout=3600,
#     dag=dag,
# )

# -------------------------------------------------------
# Task 2: Run Feature Engineering
# -------------------------------------------------------

compute_features = BashOperator(
    task_id="compute_features",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        f"python -m features.feature_engineering "
        f"--data-dir data/synthetic "
        f"--output-dir data/features "
        f"--format both"
    ),
    dag=dag,
)

# -------------------------------------------------------
# Task 3: Validate Features
# -------------------------------------------------------

def _validate_features(**kwargs):
    from pathlib import Path

    import pandas as pd

    features_path = Path(f"{PROJECT_ROOT}/data/features/account_features.parquet")
    if not features_path.exists():
        raise FileNotFoundError("Feature matrix not found!")

    df = pd.read_parquet(features_path)

    checks = []
    # Row count
    if len(df) < 4500:
        checks.append(f"Row count too low: {len(df)}")

    # Column count
    if len(df.columns) < 30:
        checks.append(f"Column count too low: {len(df.columns)}")

    # Null check
    null_cols = df.columns[df.isnull().any()].tolist()
    numeric_nulls = df.select_dtypes(include=["number"]).isnull().sum().sum()
    if numeric_nulls > 0:
        checks.append(f"Numeric nulls found: {numeric_nulls} in columns {null_cols}")

    # Target present
    if "clv_12m" not in df.columns:
        checks.append("Missing target column: clv_12m")

    if checks:
        raise ValueError("Feature validation failed:\n" + "\n".join(f"  - {c}" for c in checks))

    print(f"✅ Feature validation passed: {len(df)} rows × {len(df.columns)} columns")
    kwargs["ti"].xcom_push(key="feature_shape", value={"rows": len(df), "cols": len(df.columns)})


validate_features = PythonOperator(
    task_id="validate_features",
    python_callable=_validate_features,
    dag=dag,
)

# -------------------------------------------------------
# Task 4: Log Feature Stats
# -------------------------------------------------------

def _log_feature_stats(**kwargs):
    from pathlib import Path

    import pandas as pd

    df = pd.read_parquet(Path(f"{PROJECT_ROOT}/data/features/account_features.parquet"))
    numeric = df.select_dtypes(include=["number"])
    stats = numeric.describe().T[["mean", "std", "min", "max"]]

    print("=" * 60)
    print("FEATURE ENGINEERING PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Shape: {df.shape[0]} accounts × {df.shape[1]} features")
    print(f"\nFeature Statistics:\n{stats.to_string()}")
    print("=" * 60)


log_stats = PythonOperator(
    task_id="log_feature_stats",
    python_callable=_log_feature_stats,
    dag=dag,
)

# -------------------------------------------------------
# Dependencies
# -------------------------------------------------------

compute_features >> validate_features >> log_stats
