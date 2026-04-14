"""
Survival Analysis — Churn Risk Model
======================================
Uses Cox Proportional Hazards and Kaplan-Meier estimators from the `lifelines`
library to model customer tenure and predict churn probability over time.

Usage:
    python -m models.survival_model --features data/features/account_features.parquet
"""

import argparse
import logging
import warnings
from pathlib import Path

import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Features to use for Cox PH (avoid multicollinearity — pick one per group)
COX_FEATURES = [
    "tier_rank",
    "tenure_days",
    "annual_contract_value",
    "booking_count_90d",
    "total_spend_90d",
    "booking_volume_trend",
    "spend_acceleration",
    "cancellation_rate_90d",
    "num_active_products",
    "ticket_rate_per_month",
    "p1_p2_escalation_ratio",
    "out_of_policy_rate_90d",
    "avg_resolution_hours",
    "destination_diversity",
]


def prepare_survival_data(features_path: Path):
    """Prepare data for survival analysis."""
    logger.info("Loading features from %s", features_path)

    if features_path.suffix == ".parquet":
        df = pd.read_parquet(features_path)
    else:
        df = pd.read_csv(features_path)

    # Duration = tenure_days (time observed)
    # Event = is_churned (1 = churned, 0 = censored/still active)
    survival_df = df[COX_FEATURES + ["account_id", "is_churned"]].copy()
    survival_df["duration"] = df["tenure_days"].clip(lower=1)  # must be > 0
    survival_df["event"] = df["is_churned"].astype(int)

    # Standardize features for Cox PH convergence
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler()
    survival_df[COX_FEATURES] = scaler.fit_transform(survival_df[COX_FEATURES])

    logger.info("Survival data: %d accounts | %.1f%% churned", len(survival_df), survival_df["event"].mean() * 100)

    return survival_df, scaler


def fit_kaplan_meier(survival_df: pd.DataFrame, output_dir: Path):
    """Fit Kaplan-Meier estimator and generate survival curves by tier."""
    from lifelines import KaplanMeierFitter

    logger.info("Fitting Kaplan-Meier estimator...")

    kmf = KaplanMeierFitter()

    # Overall survival curve
    kmf.fit(survival_df["duration"], event_observed=survival_df["event"], label="All Accounts")

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Plot 1: Overall survival
    kmf.plot_survival_function(ax=axes[0])
    axes[0].set_title("Overall Customer Survival Curve", fontsize=14, fontweight="bold")
    axes[0].set_xlabel("Days since Onboarding")
    axes[0].set_ylabel("Survival Probability")
    axes[0].grid(alpha=0.3)

    # Plot 2: Survival by tier
    # Use original (non-standardized) tier_rank to split: map standardized back to groups
    tier_rank_raw = survival_df["tier_rank"].copy()
    # Since tier_rank is standardized, use simple percentile-based grouping
    try:
        tier_groups = pd.qcut(tier_rank_raw, q=4, labels=["Bronze", "Silver", "Gold", "Platinum"], duplicates="drop")
    except ValueError:
        # If qcut fails due to too few unique values, use cut with the unique values
        tier_groups = pd.cut(tier_rank_raw, bins=3, labels=["Low", "Medium", "High"])

    for tier_label in tier_groups.dropna().unique():
        mask = tier_groups == tier_label
        if mask.sum() > 10:
            kmf_tier = KaplanMeierFitter()
            kmf_tier.fit(
                survival_df.loc[mask, "duration"],
                event_observed=survival_df.loc[mask, "event"],
                label=tier_label,
            )
            kmf_tier.plot_survival_function(ax=axes[1])

    axes[1].set_title("Survival by Client Tier", fontsize=14, fontweight="bold")
    axes[1].set_xlabel("Days since Onboarding")
    axes[1].set_ylabel("Survival Probability")
    axes[1].legend(title="Tier")
    axes[1].grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / "kaplan_meier_curves.png", dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("  → Kaplan-Meier curves saved")

    # Median survival time
    median_survival = kmf.median_survival_time_
    logger.info("  → Median survival time: %.0f days", median_survival)

    return kmf


def fit_cox_model(survival_df: pd.DataFrame, output_dir: Path):
    """Fit Cox Proportional Hazards model."""
    from lifelines import CoxPHFitter

    logger.info("Fitting Cox PH model...")

    cox_data = survival_df[COX_FEATURES + ["duration", "event"]].copy()

    cph = CoxPHFitter(penalizer=0.01)  # L2 regularization
    cph.fit(cox_data, duration_col="duration", event_col="event")

    # Print summary
    logger.info("\nCox PH Summary:\n%s", cph.summary.to_string())

    # Save summary
    cph.summary.to_csv(output_dir / "cox_ph_summary.csv")

    # Plot coefficients
    fig, ax = plt.subplots(figsize=(10, 8))
    cph.plot(ax=ax)
    ax.set_title("Cox PH Hazard Ratios", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_dir / "cox_ph_coefficients.png", dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("  → Cox PH coefficient plot saved")

    # Concordance index
    logger.info("  → Concordance Index: %.4f", cph.concordance_index_)

    return cph


def predict_churn_risk(cph, survival_df: pd.DataFrame, output_dir: Path):
    """Generate churn risk scores and survival probabilities for all accounts."""
    logger.info("Generating churn risk predictions...")

    cox_features = survival_df[COX_FEATURES].copy()

    # Predict survival function for each account at key time horizons
    time_horizons = [30, 90, 180, 365]  # days
    survival_probs = {}

    for t in time_horizons:
        sf = cph.predict_survival_function(cox_features, times=[t])
        survival_probs[f"survival_prob_{t}d"] = sf.iloc[0].values

    # Churn risk score = 1 - P(survival at 365 days)
    survival_probs["churn_risk_score"] = 1 - survival_probs["survival_prob_365d"]

    # Partial hazard (relative risk)
    survival_probs["partial_hazard"] = cph.predict_partial_hazard(cox_features).values.flatten()

    # Expected remaining lifetime
    try:
        expected_lifetime = cph.predict_expectation(cox_features)
        survival_probs["expected_lifetime_days"] = expected_lifetime.values.flatten()
    except Exception:
        survival_probs["expected_lifetime_days"] = np.nan

    # Combine with account IDs
    risk_df = pd.DataFrame(survival_probs)
    risk_df["account_id"] = survival_df["account_id"].values
    risk_df = risk_df[["account_id"] + [c for c in risk_df.columns if c != "account_id"]]

    # Save
    risk_df.to_csv(output_dir / "churn_risk_predictions.csv", index=False)
    risk_df.to_parquet(output_dir / "churn_risk_predictions.parquet", index=False)
    logger.info("  → Churn risk predictions saved for %d accounts", len(risk_df))

    # Risk distribution
    logger.info(
        "  → Churn risk stats: mean=%.3f, median=%.3f, high-risk (>0.5)=%d accounts",
        risk_df["churn_risk_score"].mean(),
        risk_df["churn_risk_score"].median(),
        (risk_df["churn_risk_score"] > 0.5).sum(),
    )

    # Plot churn risk distribution
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(risk_df["churn_risk_score"], bins=50, edgecolor="black", alpha=0.7, color="#e74c3c")
    ax.set_xlabel("Churn Risk Score (1 - P(Survive 365d))", fontsize=12)
    ax.set_ylabel("Number of Accounts", fontsize=12)
    ax.set_title("Churn Risk Score Distribution", fontsize=14, fontweight="bold")
    ax.axvline(x=0.5, color="black", linestyle="--", label="High-Risk Threshold")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / "churn_risk_distribution.png", dpi=150, bbox_inches="tight")
    plt.close()

    return risk_df


def log_to_mlflow(model, metrics, params, model_name, output_dir):
    """Log survival model experiment to MLflow (Local or Remote Databricks)."""
    try:
        import os

        import mlflow

        if os.environ.get("DATABRICKS_HOST"):
            logger.info("Remote Databricks environment detected. Configuring MLFlow tracking...")
            mlflow.set_tracking_uri("databricks")

            user_email = os.environ.get("DATABRICKS_USER_EMAIL", "amex-gbt-dev")
            experiment_path = f"/Users/{user_email}/survival_analysis/{model_name}"
            mlflow.set_experiment(experiment_path)
        else:
            mlflow.set_experiment("Survival-Analysis")

        with mlflow.start_run(run_name=model_name):
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)

            # Log plots as artifacts
            for plot_file in output_dir.glob("*.png"):
                mlflow.log_artifact(str(plot_file), artifact_path="plots")

            # Log CSV summaries
            for csv_file in output_dir.glob("*.csv"):
                mlflow.log_artifact(str(csv_file), artifact_path="data")

            # Log the model file manually as a generic artifact (lifelines has no native flavor)
            mlflow.log_artifact(str(output_dir / "cox_ph_model.joblib"), artifact_path="model")

            logger.info("  → Successfully logged to MLflow: run=%s", model_name)

    except ImportError:
        logger.warning("MLflow not installed — skipping experiment logging")
    except Exception as e:
        logger.warning("MLflow logging failed: %s", e)


def main():
    parser = argparse.ArgumentParser(description="Train survival analysis models for churn prediction")
    parser.add_argument("--features", type=str, default="data/features/account_features.parquet")
    parser.add_argument("--output-dir", type=str, default="models/artifacts/survival")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Survival Analysis — Churn Risk Model")
    logger.info("=" * 60)

    # Prepare data
    survival_df, scaler = prepare_survival_data(Path(args.features))

    # Fit models
    kmf = fit_kaplan_meier(survival_df, output_dir)
    cph = fit_cox_model(survival_df, output_dir)

    # Generate predictions
    predict_churn_risk(cph, survival_df, output_dir)

    # Save model artifacts
    joblib.dump(cph, output_dir / "cox_ph_model.joblib")
    joblib.dump(scaler, output_dir / "survival_scaler.joblib")
    joblib.dump(kmf, output_dir / "kaplan_meier_model.joblib")
    logger.info("Model artifacts saved to %s", output_dir)

    # MLflow logging
    params = {
        "penalizer": 0.01,
        "n_features": len(COX_FEATURES),
        "median_survival_days": float(kmf.median_survival_time_),
    }
    metrics = {
        "concordance_index": float(cph.concordance_index_),
    }
    log_to_mlflow(cph, metrics, params, "Cox-PH-Survival", output_dir)

    logger.info("=" * 60)
    logger.info("SURVIVAL ANALYSIS COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
