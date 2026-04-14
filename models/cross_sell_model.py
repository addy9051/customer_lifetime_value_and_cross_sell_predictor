"""
Cross-Sell Propensity Model (Next Best Action)
================================================
Multi-label classifier predicting which additional Amex GBT products each
corporate account is likely to adopt next.

Target Products:
  - Neo (travel management platform)
  - Egencia Analytics Studio (business intelligence)
  - Meetings & Events (event management)
  - Travel Consulting (advisory services)

Usage:
    python -m models.cross_sell_model --features data/features/account_features.parquet
"""

import argparse
import json
import logging
import warnings
from pathlib import Path

import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    precision_recall_curve,
    average_precision_score,
)
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputClassifier

warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Product columns (binary adoption flags)
PRODUCT_COLS = [
    "has_neo",
    "has_egencia_analytics_studio",
    "has_meetings_and_events",
    "has_travel_consulting",
]

PRODUCT_NAMES = ["Neo", "Egencia Analytics Studio", "Meetings & Events", "Travel Consulting"]

# Features to exclude
EXCLUDE_COLS = [
    "account_id", "tier", "region", "industry", "is_churned",
    "clv_12m", "feature_timestamp",
] + PRODUCT_COLS  # Don't use current adoption as feature for predicting adoption


def prepare_data(features_path: Path):
    """Load and prepare multi-label classification data."""
    logger.info("Loading features from %s", features_path)

    if features_path.suffix == ".parquet":
        df = pd.read_parquet(features_path)
    else:
        df = pd.read_csv(features_path)

    feature_cols = [c for c in df.columns if c not in EXCLUDE_COLS]
    X = df[feature_cols].copy().fillna(0)

    # Drop any remaining datetime/object columns (safety net for XGBoost compatibility)
    datetime_cols = X.select_dtypes(include=["datetime64", "datetimetz", "object"]).columns.tolist()
    if datetime_cols:
        logger.info("Dropping non-numeric columns: %s", datetime_cols)
        X = X.drop(columns=datetime_cols)

    Y = df[PRODUCT_COLS].copy().fillna(0).astype(int)

    logger.info("Feature matrix: %d accounts × %d features", X.shape[0], X.shape[1])
    logger.info("Target matrix: %d accounts × %d products", Y.shape[0], Y.shape[1])
    logger.info("Product adoption rates:")
    for col, name in zip(PRODUCT_COLS, PRODUCT_NAMES):
        logger.info("  %s: %.1f%%", name, Y[col].mean() * 100)

    X_train, X_test, Y_train, Y_test = train_test_split(
        X, Y, test_size=0.2, random_state=42,
    )

    logger.info("Splits — Train: %d | Test: %d", len(X_train), len(X_test))
    return X_train, X_test, Y_train, Y_test, df, X.columns.tolist()


def train_cross_sell_model(X_train, Y_train, X_test, Y_test):
    """Train multi-output XGBoost with probability calibration."""
    import xgboost as xgb

    logger.info("Training multi-label XGBoost classifier...")

    base_model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=5,  # Handle class imbalance (low adoption rates)
        random_state=42,
        n_jobs=-1,
        verbosity=0,
        eval_metric="logloss",
    )

    multi_model = MultiOutputClassifier(base_model, n_jobs=1)
    multi_model.fit(X_train, Y_train)

    logger.info("  → Multi-label model trained with %d outputs", len(PRODUCT_COLS))

    return multi_model


def evaluate_cross_sell(model, X_test, Y_test, output_dir: Path):
    """Evaluate multi-label model with per-product metrics."""
    logger.info("Evaluating cross-sell model...")

    Y_pred = model.predict(X_test)
    Y_pred_proba = np.column_stack([
        est.predict_proba(X_test)[:, 1] for est in model.estimators_
    ])

    # Per-product metrics
    results = []
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    for i, (col, name) in enumerate(zip(PRODUCT_COLS, PRODUCT_NAMES)):
        y_true = Y_test[col].values
        y_pred = Y_pred[:, i]
        y_proba = Y_pred_proba[:, i]

        # AUC-ROC
        try:
            auc = roc_auc_score(y_true, y_proba)
        except ValueError:
            auc = 0.0

        # Average Precision
        ap = average_precision_score(y_true, y_proba)

        report = classification_report(y_true, y_pred, output_dict=True, zero_division=0)

        results.append({
            "product": name,
            "auc_roc": round(auc, 4),
            "avg_precision": round(ap, 4),
            "precision": round(report.get("1", {}).get("precision", 0), 4),
            "recall": round(report.get("1", {}).get("recall", 0), 4),
            "f1": round(report.get("1", {}).get("f1-score", 0), 4),
            "support": int(report.get("1", {}).get("support", 0)),
        })

        # Precision-Recall curve
        ax = axes[i // 2][i % 2]
        precision, recall, _ = precision_recall_curve(y_true, y_proba)
        ax.plot(recall, precision, linewidth=2, color="#3498db")
        ax.fill_between(recall, precision, alpha=0.2, color="#3498db")
        ax.set_title(f"{name}\nAUC-ROC={auc:.3f} | AP={ap:.3f}", fontsize=11, fontweight="bold")
        ax.set_xlabel("Recall")
        ax.set_ylabel("Precision")
        ax.grid(alpha=0.3)
        ax.set_xlim([0, 1])
        ax.set_ylim([0, 1])

        logger.info("  %s → AUC=%.3f | AP=%.3f | F1=%.3f", name, auc, ap, report.get("1", {}).get("f1-score", 0))

    plt.suptitle("Cross-Sell Precision-Recall Curves", fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(output_dir / "cross_sell_pr_curves.png", dpi=150, bbox_inches="tight")
    plt.close()

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_dir / "cross_sell_metrics.csv", index=False)
    logger.info("\nCross-Sell Metrics:\n%s", results_df.to_string(index=False))

    return results_df, Y_pred_proba


def generate_recommendations(
    model,
    df: pd.DataFrame,
    feature_cols: list,
    output_dir: Path,
):
    """Generate ranked product recommendations for all accounts."""
    logger.info("Generating recommendations for all accounts...")

    X_all = df[feature_cols].copy().fillna(0)

    # Predict probabilities
    proba = np.column_stack([
        est.predict_proba(X_all)[:, 1] for est in model.estimators_
    ])

    # Build recommendation matrix
    recs = pd.DataFrame(proba, columns=[f"{name}_score" for name in PRODUCT_NAMES])
    recs["account_id"] = df["account_id"].values

    # Add current adoption status
    for col, name in zip(PRODUCT_COLS, PRODUCT_NAMES):
        recs[f"{name}_current"] = df[col].values

    # For each account, rank products not yet adopted
    top_recs = []
    for _, row in recs.iterrows():
        account_recs = []
        for name in PRODUCT_NAMES:
            if row[f"{name}_current"] == 0:  # Not yet adopted
                account_recs.append({
                    "product": name,
                    "score": row[f"{name}_score"],
                })

        # Sort by score descending
        account_recs.sort(key=lambda x: x["score"], reverse=True)

        top_recs.append({
            "account_id": row["account_id"],
            "top_1_product": account_recs[0]["product"] if len(account_recs) > 0 else None,
            "top_1_score": account_recs[0]["score"] if len(account_recs) > 0 else 0,
            "top_2_product": account_recs[1]["product"] if len(account_recs) > 1 else None,
            "top_2_score": account_recs[1]["score"] if len(account_recs) > 1 else 0,
            "num_products_current": sum(row[f"{name}_current"] for name in PRODUCT_NAMES),
        })

    rec_df = pd.DataFrame(top_recs)
    rec_df.to_csv(output_dir / "account_recommendations.csv", index=False)
    rec_df.to_parquet(output_dir / "account_recommendations.parquet", index=False)

    # Also save full probability matrix
    recs.to_csv(output_dir / "cross_sell_probabilities.csv", index=False)
    recs.to_parquet(output_dir / "cross_sell_probabilities.parquet", index=False)

    logger.info("  → Recommendations saved for %d accounts", len(rec_df))

    # Top recommendation distribution
    if rec_df["top_1_product"].notna().any():
        top_dist = rec_df["top_1_product"].value_counts()
        logger.info("Top-1 recommendation distribution:\n%s", top_dist.to_string())

    return rec_df


def main():
    parser = argparse.ArgumentParser(description="Train cross-sell propensity model")
    parser.add_argument("--features", type=str, default="data/features/account_features.parquet")
    parser.add_argument("--output-dir", type=str, default="models/artifacts/cross_sell")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Cross-Sell Propensity Model Pipeline")
    logger.info("=" * 60)

    # Prepare data
    X_train, X_test, Y_train, Y_test, df, feature_cols = prepare_data(Path(args.features))

    # Train model
    model = train_cross_sell_model(X_train, Y_train, X_test, Y_test)

    # Evaluate
    metrics_df, proba = evaluate_cross_sell(model, X_test, Y_test, output_dir)

    # Generate recommendations for all accounts
    rec_df = generate_recommendations(model, df, feature_cols, output_dir)

    # Save model
    joblib.dump(model, output_dir / "cross_sell_model.joblib")

    # Save feature list
    with open(output_dir / "feature_columns.json", "w") as f:
        json.dump(feature_cols, f, indent=2)

    logger.info("=" * 60)
    logger.info("CROSS-SELL MODEL TRAINING COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
