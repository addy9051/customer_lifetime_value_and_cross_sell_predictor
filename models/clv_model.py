"""
CLV Prediction Model
=====================
Trains XGBoost and LightGBM regressors to predict 12-month Customer Lifetime Value.
Includes hyperparameter tuning (Optuna), SHAP explanations, and MLflow tracking.

Usage:
    python -m models.clv_model --features data/features/account_features.parquet
"""

import argparse
import json
import logging
import os
import warnings
from pathlib import Path

import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

warnings.filterwarnings("ignore", category=FutureWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Features to exclude from training (identifiers, targets, metadata)
EXCLUDE_COLS = [
    "account_id",
    "tier",
    "region",
    "industry",
    "is_churned",
    "clv_12m",
    "feature_timestamp",
]

# Target column
TARGET = "clv_12m"


def prepare_data(features_path: Path):
    """Load feature matrix and prepare train/val/test splits."""
    logger.info("Loading features from %s", features_path)

    if features_path.suffix == ".parquet":
        df = pd.read_parquet(features_path)
    else:
        df = pd.read_csv(features_path)

    logger.info("Feature matrix: %d rows × %d columns", df.shape[0], df.shape[1])

    # Separate features and target
    feature_cols = [c for c in df.columns if c not in EXCLUDE_COLS]
    X = df[feature_cols].copy()

    # Drop any remaining datetime/object columns (safety net for XGBoost compatibility)
    datetime_cols = X.select_dtypes(include=["datetime64", "datetimetz", "object"]).columns.tolist()
    if datetime_cols:
        logger.info("Dropping non-numeric columns: %s", datetime_cols)
        X = X.drop(columns=datetime_cols)
    y = df[TARGET].copy()

    # Log-transform target for better regression performance (CLV is right-skewed)
    y_log = np.log1p(y)

    # Stratified split by tier for balanced representation
    tier_encoded = LabelEncoder().fit_transform(df["tier"])

    X_train, X_temp, y_train, y_temp, tier_train, tier_temp = train_test_split(
        X,
        y_log,
        tier_encoded,
        test_size=0.30,
        random_state=42,
        stratify=tier_encoded,
    )
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp,
        y_temp,
        test_size=0.50,
        random_state=42,
    )

    logger.info("Splits — Train: %d | Val: %d | Test: %d", len(X_train), len(X_val), len(X_test))

    return X_train, X_val, X_test, y_train, y_val, y_test, df


def train_xgboost(X_train, y_train, X_val, y_val, tune: bool = True):
    """Train XGBoost regressor with optional Optuna tuning."""
    import xgboost as xgb

    logger.info("Training XGBoost regressor...")

    if tune:
        try:
            import optuna

            optuna.logging.set_verbosity(optuna.logging.WARNING)

            def objective(trial):
                params = {
                    "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
                    "max_depth": trial.suggest_int("max_depth", 3, 10),
                    "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                    "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                    "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
                    "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
                    "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
                    "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
                }
                model = xgb.XGBRegressor(
                    **params,
                    random_state=42,
                    n_jobs=-1,
                    verbosity=0,
                    early_stopping_rounds=50,
                )
                model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
                preds = model.predict(X_val)
                return mean_squared_error(y_val, preds, squared=False)

            study = optuna.create_study(direction="minimize")
            study.optimize(objective, n_trials=50, show_progress_bar=True)
            best_params = study.best_params
            logger.info("Optuna best params: %s", best_params)
            logger.info("Optuna best RMSE (log-space): %.4f", study.best_value)
        except ImportError:
            logger.warning("Optuna not installed — using default hyperparameters")
            best_params = {
                "n_estimators": 500,
                "max_depth": 6,
                "learning_rate": 0.05,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
            }
    else:
        best_params = {
            "n_estimators": 500,
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
        }

    model = xgb.XGBRegressor(
        **best_params,
        random_state=42,
        n_jobs=-1,
        verbosity=0,
        early_stopping_rounds=50,
    )
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

    return model, best_params


def train_lightgbm(X_train, y_train, X_val, y_val):
    """Train LightGBM regressor as a benchmark."""
    import lightgbm as lgb

    logger.info("Training LightGBM benchmark...")

    params = {
        "n_estimators": 500,
        "max_depth": 6,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
        "n_jobs": -1,
        "verbose": -1,
    }

    model = lgb.LGBMRegressor(**params)
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(0)],
    )

    return model, params


def evaluate_model(model, X_test, y_test, model_name: str) -> dict:
    """Evaluate model on test set. Returns metrics in original dollar space."""
    y_pred_log = model.predict(X_test)

    # Transform back to original space
    y_pred = np.expm1(y_pred_log)
    y_true = np.expm1(y_test)

    # Clip negative predictions
    y_pred = np.clip(y_pred, 0, None)

    metrics = {
        "model": model_name,
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "r2": float(r2_score(y_true, y_pred)),
        "mape": float(np.mean(np.abs((y_true - y_pred) / (y_true + 1))) * 100),
        "median_ae": float(np.median(np.abs(y_true - y_pred))),
    }

    logger.info(
        "  %s → MAE=$%.0f | RMSE=$%.0f | R²=%.4f | MAPE=%.1f%%",
        model_name,
        metrics["mae"],
        metrics["rmse"],
        metrics["r2"],
        metrics["mape"],
    )
    return metrics


def generate_shap_explanations(model, X_test, output_dir: Path):
    """Generate SHAP explanations for the XGBoost model."""
    try:
        import shap

        logger.info("Generating SHAP explanations...")
        explainer = shap.TreeExplainer(model)

        # Use a sample for speed
        sample_size = min(500, len(X_test))
        X_sample = X_test.iloc[:sample_size]
        shap_values = explainer.shap_values(X_sample)

        # Summary plot (bar)
        fig, ax = plt.subplots(figsize=(12, 8))
        shap.summary_plot(shap_values, X_sample, plot_type="bar", show=False, max_display=20)
        plt.tight_layout()
        plt.savefig(output_dir / "shap_importance_bar.png", dpi=150, bbox_inches="tight")
        plt.close()

        # Summary plot (beeswarm)
        fig, ax = plt.subplots(figsize=(12, 8))
        shap.summary_plot(shap_values, X_sample, show=False, max_display=20)
        plt.tight_layout()
        plt.savefig(output_dir / "shap_beeswarm.png", dpi=150, bbox_inches="tight")
        plt.close()

        logger.info("  → SHAP plots saved to %s", output_dir)

    except ImportError:
        logger.warning("SHAP not installed — skipping explanations")


def log_to_mlflow(model, metrics, params, model_name, output_dir):
    """Log experiment to MLflow (Local or Remote Databricks)."""
    try:
        import mlflow
        import mlflow.lightgbm
        import mlflow.xgboost

        # Check for remote Databricks tracking
        if os.environ.get("DATABRICKS_HOST"):
            logger.info("Remote Databricks environment detected. Configuring MLFlow tracking...")
            mlflow.set_tracking_uri("databricks")

            # Use a standardized Databricks workspace path
            # In a real environment, this would be customized per user
            user_email = os.environ.get("DATABRICKS_USER_EMAIL", "amex-gbt-dev")
            experiment_path = f"/Users/{user_email}/clv_prediction/{model_name}"
            mlflow.set_experiment(experiment_path)
        else:
            mlflow.set_experiment("CLV-Prediction")

        with mlflow.start_run(run_name=model_name):
            mlflow.log_params(params)
            mlflow.log_metrics({k: v for k, v in metrics.items() if k != "model"})

            if "xgb" in model_name.lower():
                # For XGBoost, we log the native model
                mlflow.xgboost.log_model(
                    model,
                    artifact_path="model",
                    registered_model_name="amex-gbt-clv" if os.environ.get("DATABRICKS_HOST") else None,
                )
            else:
                mlflow.lightgbm.log_model(
                    model,
                    artifact_path="model",
                    registered_model_name="amex-gbt-lgbm-clv" if os.environ.get("DATABRICKS_HOST") else None,
                )

            # Log SHAP plots as artifacts
            for plot_file in output_dir.glob("shap_*.png"):
                mlflow.log_artifact(str(plot_file), artifact_path="shap")

            logger.info("  → Successfully logged to MLflow: run=%s", model_name)

    except ImportError:
        logger.warning("MLflow not installed — skipping experiment logging")
    except Exception as e:
        logger.warning("MLflow logging failed: %s", e)


def main():
    parser = argparse.ArgumentParser(description="Train CLV prediction models")
    parser.add_argument("--features", type=str, default="data/features/account_features.parquet")
    parser.add_argument("--output-dir", type=str, default="models/artifacts/clv")
    parser.add_argument("--no-tune", action="store_true", help="Skip Optuna hyperparameter tuning")
    parser.add_argument("--no-lgbm", action="store_true", help="Skip LightGBM benchmark")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("CLV Model Training Pipeline")
    logger.info("=" * 60)

    # Prepare data
    X_train, X_val, X_test, y_train, y_val, y_test, full_df = prepare_data(Path(args.features))

    # Train XGBoost
    xgb_model, xgb_params = train_xgboost(X_train, y_train, X_val, y_val, tune=not args.no_tune)
    xgb_metrics = evaluate_model(xgb_model, X_test, y_test, "XGBoost-CLV")

    # Save XGBoost model
    joblib.dump(xgb_model, output_dir / "xgb_clv_model.joblib")
    logger.info("Saved XGBoost model → %s", output_dir / "xgb_clv_model.joblib")

    # SHAP explanations
    generate_shap_explanations(xgb_model, X_test, output_dir)

    # MLflow logging
    log_to_mlflow(xgb_model, xgb_metrics, xgb_params, "XGBoost-CLV", output_dir)

    # Feature importance
    importance = pd.DataFrame(
        {
            "feature": X_train.columns,
            "importance": xgb_model.feature_importances_,
        }
    ).sort_values("importance", ascending=False)
    importance.to_csv(output_dir / "feature_importance.csv", index=False)
    logger.info("Top 10 features:\n%s", importance.head(10).to_string(index=False))

    all_metrics = [xgb_metrics]

    # Train LightGBM benchmark
    if not args.no_lgbm:
        try:
            lgbm_model, lgbm_params = train_lightgbm(X_train, y_train, X_val, y_val)
            lgbm_metrics = evaluate_model(lgbm_model, X_test, y_test, "LightGBM-CLV")
            joblib.dump(lgbm_model, output_dir / "lgbm_clv_model.joblib")
            log_to_mlflow(lgbm_model, lgbm_metrics, lgbm_params, "LightGBM-CLV", output_dir)
            all_metrics.append(lgbm_metrics)
        except ImportError:
            logger.warning("LightGBM not installed — skipping benchmark")

    # Save metrics comparison
    metrics_df = pd.DataFrame(all_metrics)
    metrics_df.to_csv(output_dir / "model_comparison.csv", index=False)
    logger.info("\nModel Comparison:\n%s", metrics_df.to_string(index=False))

    # Save feature list for serving
    feature_list = list(X_train.columns)
    with open(output_dir / "feature_columns.json", "w") as f:
        json.dump(feature_list, f, indent=2)

    logger.info("=" * 60)
    logger.info("CLV MODEL TRAINING COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
