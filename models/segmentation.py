"""
Client Segmentation Model
===========================
Segments corporate accounts into actionable business tiers using
UMAP for dimensionality reduction and HDBSCAN for density-based clustering.

Segments are labeled post-hoc using business rules:
  🏆 Platinum Partners — High CLV, low churn, full product adoption
  📈 Growth Accounts — Medium CLV, positive trajectory, cross-sell opportunity
  ⚠️  At-Risk Accounts — Declining bookings, rising tickets, churn signals
  🔻 Low-Engagement — Minimal activity, single product, low contract value

Usage:
    python -m models.segmentation --features data/features/account_features.parquet
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
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, calinski_harabasz_score

warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Features used for segmentation (behavioral + value signals)
SEGMENT_FEATURES = [
    "tier_rank",
    "annual_contract_value",
    "tenure_days",
    "booking_count_90d",
    "total_spend_90d",
    "booking_volume_trend",
    "spend_acceleration",
    "cancellation_rate_90d",
    "num_active_products",
    "ticket_rate_per_month",
    "p1_p2_escalation_ratio",
    "out_of_policy_rate_90d",
    "clv_12m",
]


def prepare_segmentation_data(features_path: Path):
    """Load and standardize features for segmentation."""
    logger.info("Loading features from %s", features_path)

    if features_path.suffix == ".parquet":
        df = pd.read_parquet(features_path)
    else:
        df = pd.read_csv(features_path)

    X = df[SEGMENT_FEATURES].copy().fillna(0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    logger.info("Segmentation input: %d accounts × %d features", X_scaled.shape[0], X_scaled.shape[1])
    return df, X_scaled, scaler


def run_umap(X_scaled: np.ndarray, output_dir: Path):
    """Reduce to 2D using UMAP for visualization."""
    import umap

    logger.info("Running UMAP dimensionality reduction...")

    reducer = umap.UMAP(
        n_components=2,
        n_neighbors=30,
        min_dist=0.3,
        metric="euclidean",
        random_state=42,
    )
    embedding = reducer.fit_transform(X_scaled)

    logger.info("  → UMAP embedding shape: %s", embedding.shape)
    joblib.dump(reducer, output_dir / "umap_reducer.joblib")

    return embedding, reducer


def run_hdbscan(X_scaled: np.ndarray, output_dir: Path):
    """Cluster accounts using HDBSCAN."""
    import hdbscan

    logger.info("Running HDBSCAN clustering...")

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=100,
        min_samples=20,
        cluster_selection_method="eom",
        prediction_data=True,
    )
    labels = clusterer.fit_predict(X_scaled)

    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    noise_pct = (labels == -1).mean() * 100

    logger.info("  → Clusters found: %d | Noise: %.1f%%", n_clusters, noise_pct)

    # Cluster sizes
    for label in sorted(set(labels)):
        count = (labels == label).sum()
        logger.info("    Cluster %d: %d accounts (%.1f%%)", label, count, count / len(labels) * 100)

    # Validation metrics (excluding noise)
    valid_mask = labels != -1
    if len(set(labels[valid_mask])) > 1:
        sil = silhouette_score(X_scaled[valid_mask], labels[valid_mask])
        ch = calinski_harabasz_score(X_scaled[valid_mask], labels[valid_mask])
        logger.info("  → Silhouette Score: %.4f | Calinski-Harabasz: %.1f", sil, ch)
    else:
        sil, ch = 0, 0
        logger.warning("  → Only 1 cluster found (excluding noise) — metrics not meaningful")

    joblib.dump(clusterer, output_dir / "hdbscan_clusterer.joblib")

    return labels, clusterer, {"silhouette": sil, "calinski_harabasz": ch}


def label_segments(df: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
    """Map HDBSCAN clusters to business-meaningful segments using post-hoc rules."""
    logger.info("Labeling segments with business rules...")

    df = df.copy()
    df["cluster"] = labels

    # Compute cluster-level statistics for labeling
    cluster_stats = df.groupby("cluster").agg({
        "clv_12m": "median",
        "booking_volume_trend": "median",
        "cancellation_rate_90d": "median",
        "ticket_rate_per_month": "median",
        "num_active_products": "median",
        "total_spend_90d": "median",
        "is_churned": "mean",
    }).round(3)

    logger.info("Cluster statistics:\n%s", cluster_stats.to_string())

    # Score each cluster on value dimensions
    cluster_labels = {}
    for cluster_id in cluster_stats.index:
        if cluster_id == -1:
            cluster_labels[cluster_id] = "Unassigned"
            continue

        stats = cluster_stats.loc[cluster_id]
        clv_rank = (cluster_stats["clv_12m"].rank(ascending=True) / len(cluster_stats)).get(cluster_id, 0.5)
        churn_risk = stats["is_churned"]
        trend = stats["booking_volume_trend"]
        products = stats["num_active_products"]

        # Decision tree for labeling
        if clv_rank >= 0.7 and churn_risk < 0.2 and products >= 2:
            cluster_labels[cluster_id] = "Platinum Partners"
        elif trend > 0 and clv_rank >= 0.4:
            cluster_labels[cluster_id] = "Growth Accounts"
        elif churn_risk > 0.15 or stats["cancellation_rate_90d"] > 0.15 or stats["ticket_rate_per_month"] > 1.5:
            cluster_labels[cluster_id] = "At-Risk Accounts"
        else:
            cluster_labels[cluster_id] = "Low-Engagement"

    df["segment"] = df["cluster"].map(cluster_labels)

    # Segment summary
    seg_summary = df.groupby("segment").agg(
        count=("account_id", "count"),
        avg_clv=("clv_12m", "mean"),
        avg_spend_90d=("total_spend_90d", "mean"),
        churn_rate=("is_churned", "mean"),
        avg_products=("num_active_products", "mean"),
    ).round(2)
    logger.info("\nSegment Summary:\n%s", seg_summary.to_string())

    return df


def plot_segments(df: pd.DataFrame, embedding: np.ndarray, output_dir: Path):
    """Create UMAP scatter plots colored by segment."""
    logger.info("Creating segment visualizations...")

    df = df.copy()
    df["umap_x"] = embedding[:, 0]
    df["umap_y"] = embedding[:, 1]

    segment_colors = {
        "Platinum Partners": "#2ecc71",
        "Growth Accounts": "#3498db",
        "At-Risk Accounts": "#e74c3c",
        "Low-Engagement": "#95a5a6",
        "Unassigned": "#bdc3c7",
    }

    fig, axes = plt.subplots(1, 2, figsize=(20, 8))

    # Plot 1: Segments
    for segment, color in segment_colors.items():
        mask = df["segment"] == segment
        if mask.sum() > 0:
            axes[0].scatter(
                df.loc[mask, "umap_x"],
                df.loc[mask, "umap_y"],
                c=color, label=f"{segment} ({mask.sum()})",
                s=8, alpha=0.6,
            )
    axes[0].set_title("Client Segments (UMAP + HDBSCAN)", fontsize=14, fontweight="bold")
    axes[0].set_xlabel("UMAP 1")
    axes[0].set_ylabel("UMAP 2")
    axes[0].legend(markerscale=3, fontsize=10)
    axes[0].grid(alpha=0.2)

    # Plot 2: CLV heatmap
    scatter = axes[1].scatter(
        df["umap_x"], df["umap_y"],
        c=np.log1p(df["clv_12m"]),
        cmap="YlOrRd", s=8, alpha=0.6,
    )
    axes[1].set_title("CLV Distribution (UMAP)", fontsize=14, fontweight="bold")
    axes[1].set_xlabel("UMAP 1")
    axes[1].set_ylabel("UMAP 2")
    plt.colorbar(scatter, ax=axes[1], label="log(CLV + 1)")
    axes[1].grid(alpha=0.2)

    plt.tight_layout()
    plt.savefig(output_dir / "segment_umap.png", dpi=150, bbox_inches="tight")
    plt.close()

    logger.info("  → Segment visualizations saved")


def log_to_mlflow(clusterer, metrics, params, model_name, output_dir):
    """Log segmentation experiment to MLflow (Local or Remote Databricks)."""
    try:
        import os
        import mlflow
        
        if os.environ.get("DATABRICKS_HOST"):
            logger.info("Remote Databricks environment detected. Configuring MLFlow tracking...")
            mlflow.set_tracking_uri("databricks")
            
            user_email = os.environ.get("DATABRICKS_USER_EMAIL", "amex-gbt-dev")
            experiment_path = f"/Users/{user_email}/client_segmentation/{model_name}"
            mlflow.set_experiment(experiment_path)
        else:
            mlflow.set_experiment("Client-Segmentation")

    except ImportError:
        logger.warning("MLflow not installed — skipping experiment logging")
        return

    try:
        with mlflow.start_run(run_name=model_name):
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            
            # Log UMAP visualization and metrics artifact
            mlflow.log_artifact(str(output_dir / "segment_umap.png"), artifact_path="plots")
            mlflow.log_artifact(str(output_dir / "cluster_metrics.csv"), artifact_path="data")
            mlflow.log_artifact(str(output_dir / "account_segments.parquet"), artifact_path="data")

            # Log the models
            mlflow.log_artifact(str(output_dir / "umap_reducer.joblib"), artifact_path="model")
            mlflow.log_artifact(str(output_dir / "hdbscan_clusterer.joblib"), artifact_path="model")
            
            logger.info("  → Successfully logged to MLflow: run=%s", model_name)
    except Exception as e:
        logger.warning("MLflow logging failed: %s", e)


def main():
    parser = argparse.ArgumentParser(description="Segment clients using UMAP + HDBSCAN")
    parser.add_argument("--features", type=str, default="data/features/account_features.parquet")
    parser.add_argument("--output-dir", type=str, default="models/artifacts/segmentation")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Client Segmentation Pipeline")
    logger.info("=" * 60)

    # Prepare data
    df, X_scaled, scaler = prepare_segmentation_data(Path(args.features))

    # UMAP
    embedding, reducer = run_umap(X_scaled, output_dir)

    # HDBSCAN
    labels, clusterer, cluster_metrics = run_hdbscan(X_scaled, output_dir)

    # Label segments
    df_segmented = label_segments(df, labels)

    # Visualize
    plot_segments(df_segmented, embedding, output_dir)

    # Save outputs
    seg_output = df_segmented[["account_id", "cluster", "segment"]].copy()
    seg_output["umap_x"] = embedding[:, 0]
    seg_output["umap_y"] = embedding[:, 1]
    seg_output.to_csv(output_dir / "account_segments.csv", index=False)
    seg_output.to_parquet(output_dir / "account_segments.parquet", index=False)

    joblib.dump(scaler, output_dir / "segment_scaler.joblib")

    # Save metrics
    pd.DataFrame([cluster_metrics]).to_csv(output_dir / "cluster_metrics.csv", index=False)

    # MLflow logging
    params = {
        "min_cluster_size": 100,
        "min_samples": 20,
        "n_features": len(SEGMENT_FEATURES),
    }
    log_to_mlflow(clusterer, cluster_metrics, params, "UMAP-HDBSCAN-Segmentation", output_dir)

    logger.info("=" * 60)
    logger.info("SEGMENTATION COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
