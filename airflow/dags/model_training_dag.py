"""
Airflow DAG: Model Retraining Pipeline
========================================
Orchestrates the weekly retraining of the four core ML models:
    1. CLV Prediction (XGBoost)
    2. Churn Survival Analysis (Cox PH)
    3. Client Segmentation (UMAP + HDBSCAN)
    4. Cross-Sell Propensity (Multi-label XGBoost)

Schedule: Runs every Sunday at 02:00 AM, waiting for Feature Engineering to complete.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Standardize path
PROJECT_ROOT = "/opt/airflow/project"

default_args = {
    "owner": "clv-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="model_training_pipeline",
    default_args=default_args,
    description="Retrain core ML models and log to Databricks MLFlow",
    schedule="0 2 * * 0",  # Every Sunday at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["clv", "model-training", "mlflow"],
)

# -------------------------------------------------------
# Task 1: Wait for Feature Engineering
# -------------------------------------------------------

wait_for_features = ExternalTaskSensor(
    task_id="wait_for_features",
    external_dag_id="feature_engineering_pipeline",
    external_task_id="log_feature_stats",
    allowed_states=["success"],
    timeout=3600,
    check_existence=True,
    dag=dag,
)

# -------------------------------------------------------
# Task 2: Model Training Operators
# -------------------------------------------------------
# Running models via bash. The models utilize DATABRICKS_*
# environment variables defined inside the Airflow Worker's environment

train_clv = BashOperator(
    task_id="train_clv_model",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        f"python -m models.clv_model "
        f"--features data/features/account_features.parquet "
        f"--output-dir models/artifacts/clv"
    ),
    dag=dag,
)

train_survival = BashOperator(
    task_id="train_survival_model",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        f"python -m models.survival_model "
        f"--features data/features/account_features.parquet "
        f"--output-dir models/artifacts/survival"
    ),
    dag=dag,
)

run_segmentation = BashOperator(
    task_id="run_segmentation",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        f"python -m models.segmentation "
        f"--features data/features/account_features.parquet "
        f"--output-dir models/artifacts/segmentation"
    ),
    dag=dag,
)

train_cross_sell = BashOperator(
    task_id="train_cross_sell_model",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        f"python -m models.cross_sell_model "
        f"--features data/features/account_features.parquet "
        f"--output-dir models/artifacts/cross_sell"
    ),
    dag=dag,
)

# -------------------------------------------------------
# Dependencies (Fan-out training)
# -------------------------------------------------------

wait_for_features >> [train_clv, train_survival, run_segmentation, train_cross_sell]
