"""
Feature Engineering Pipeline
=============================
Computes account-level ML features from raw synthetic data for CLV prediction,
churn survival analysis, segmentation, and cross-sell propensity modeling.

Feature Groups:
    1. RFM (Recency / Frequency / Monetary) — 30d, 90d, 180d windows
    2. Behavioral Trajectory — volume trends, spend acceleration, cancellation trends
    3. Service Adoption — product count, diversity, contract renewals
    4. Support Health — ticket rate, resolution time, escalation ratio
    5. Policy Compliance — out-of-policy rates and trends

Usage:
    python -m features.feature_engineering --data-dir data/synthetic --output-dir data/features
"""

import argparse
import logging
from pathlib import Path
from datetime import timedelta

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Feature computation cutoff — midpoint of observation window.
# Features are computed using data BEFORE this date.
# CLV labels use data AFTER this date (forward 12 months).
CUTOFF_DATE = pd.Timestamp("2023-01-01")

# Time windows for RFM features
WINDOWS = [30, 90, 180]


# =============================================================================
# Data Loading
# =============================================================================

def load_data(data_dir: Path) -> dict:
    """Load all entity tables from Parquet (faster) or CSV fallback."""
    tables = {}
    for name in ["corporate_accounts", "traveler_profiles", "bookings", "service_contracts", "support_tickets", "clv_labels"]:
        parquet_path = data_dir / f"{name}.parquet"
        csv_path = data_dir / f"{name}.csv"

        if parquet_path.exists():
            tables[name] = pd.read_parquet(parquet_path)
        elif csv_path.exists():
            tables[name] = pd.read_csv(csv_path)
        else:
            raise FileNotFoundError(f"Missing data file for {name}")

        logger.info("Loaded %-25s %7d rows", name, len(tables[name]))

    # Parse date columns
    date_cols = {
        "corporate_accounts": ["onboarding_date", "churn_date"],
        "bookings": ["booking_date", "travel_date"],
        "service_contracts": ["start_date", "end_date"],
        "support_tickets": ["created_date", "resolved_date"],
    }
    for table_name, cols in date_cols.items():
        for col in cols:
            if col in tables[table_name].columns:
                tables[table_name][col] = pd.to_datetime(tables[table_name][col], errors="coerce")

    return tables


# =============================================================================
# Feature Group 1: RFM Features
# =============================================================================

def compute_rfm_features(bookings: pd.DataFrame, travelers: pd.DataFrame) -> pd.DataFrame:
    """Compute Recency, Frequency, and Monetary features per account over multiple windows."""
    logger.info("Computing RFM features...")

    # Map bookings to accounts via travelers
    traveler_to_account = travelers.set_index("traveler_id")["account_id"]
    bookings = bookings.copy()
    bookings["account_id"] = bookings["traveler_id"].map(traveler_to_account)

    # Only use historical bookings (before cutoff), exclude cancelled
    hist = bookings[(bookings["booking_date"] < CUTOFF_DATE) & (~bookings["is_cancelled"])].copy()

    features = {}

    for window_days in WINDOWS:
        window_start = CUTOFF_DATE - pd.Timedelta(days=window_days)
        windowed = hist[hist["booking_date"] >= window_start]

        freq = windowed.groupby("account_id").size().rename(f"booking_count_{window_days}d")
        monetary = windowed.groupby("account_id")["amount"].sum().rename(f"total_spend_{window_days}d")
        avg_amount = windowed.groupby("account_id")["amount"].mean().rename(f"avg_booking_amount_{window_days}d")

        features[f"booking_count_{window_days}d"] = freq
        features[f"total_spend_{window_days}d"] = monetary
        features[f"avg_booking_amount_{window_days}d"] = avg_amount

    # Recency: days since last booking (before cutoff)
    last_booking = hist.groupby("account_id")["booking_date"].max()
    recency = (CUTOFF_DATE - last_booking).dt.days.rename("days_since_last_booking")
    features["days_since_last_booking"] = recency

    # Total historical stats
    features["total_booking_count"] = hist.groupby("account_id").size().rename("total_booking_count")
    features["total_spend"] = hist.groupby("account_id")["amount"].sum().rename("total_spend")

    rfm = pd.DataFrame(features)
    logger.info("  → RFM features: %d columns for %d accounts", rfm.shape[1], rfm.shape[0])
    return rfm


# =============================================================================
# Feature Group 2: Behavioral Trajectory
# =============================================================================

def compute_trajectory_features(bookings: pd.DataFrame, travelers: pd.DataFrame) -> pd.DataFrame:
    """Compute booking volume trend, spend acceleration, and cancellation rate trends."""
    logger.info("Computing behavioral trajectory features...")

    traveler_to_account = travelers.set_index("traveler_id")["account_id"]
    bookings = bookings.copy()
    bookings["account_id"] = bookings["traveler_id"].map(traveler_to_account)

    hist = bookings[bookings["booking_date"] < CUTOFF_DATE].copy()
    hist["year_month"] = hist["booking_date"].dt.to_period("M")

    features = {}

    # Monthly booking volume trend (linear regression slope)
    monthly_counts = hist.groupby(["account_id", "year_month"]).size().reset_index(name="monthly_bookings")
    monthly_counts["month_idx"] = monthly_counts.groupby("account_id").cumcount()

    def _slope(group):
        if len(group) < 3:
            return 0.0
        x = group["month_idx"].values.astype(float)
        y = group["monthly_bookings"].values.astype(float)
        # Least squares slope
        x_mean, y_mean = x.mean(), y.mean()
        denom = ((x - x_mean) ** 2).sum()
        if denom == 0:
            return 0.0
        return float(((x - x_mean) * (y - y_mean)).sum() / denom)

    volume_trend = monthly_counts.groupby("account_id").apply(_slope, include_groups=False).rename("booking_volume_trend")
    features["booking_volume_trend"] = volume_trend

    # Monthly spend trend
    monthly_spend = hist[~hist["is_cancelled"]].groupby(["account_id", "year_month"])["amount"].sum().reset_index()
    monthly_spend["month_idx"] = monthly_spend.groupby("account_id").cumcount()

    def _spend_slope(group):
        if len(group) < 3:
            return 0.0
        x = group["month_idx"].values.astype(float)
        y = group["amount"].values.astype(float)
        x_mean, y_mean = x.mean(), y.mean()
        denom = ((x - x_mean) ** 2).sum()
        if denom == 0:
            return 0.0
        return float(((x - x_mean) * (y - y_mean)).sum() / denom)

    spend_trend = monthly_spend.groupby("account_id").apply(_spend_slope, include_groups=False).rename("spend_trend")
    features["spend_trend"] = spend_trend

    # Spend acceleration (difference between recent slope and older slope)
    # Compare last 6 months vs 6-12 months ago
    six_months_ago = CUTOFF_DATE - pd.Timedelta(days=180)
    twelve_months_ago = CUTOFF_DATE - pd.Timedelta(days=365)

    recent_spend = hist[(hist["booking_date"] >= six_months_ago) & (~hist["is_cancelled"])].groupby("account_id")["amount"].mean()
    older_spend = hist[(hist["booking_date"] >= twelve_months_ago) & (hist["booking_date"] < six_months_ago) & (~hist["is_cancelled"])].groupby("account_id")["amount"].mean()

    spend_accel = (recent_spend - older_spend).fillna(0).rename("spend_acceleration")
    features["spend_acceleration"] = spend_accel

    # Cancellation rate trends (30d and 90d)
    for window_days in [30, 90]:
        window_start = CUTOFF_DATE - pd.Timedelta(days=window_days)
        windowed = hist[hist["booking_date"] >= window_start]
        total = windowed.groupby("account_id").size()
        cancelled = windowed[windowed["is_cancelled"]].groupby("account_id").size()
        cancel_rate = (cancelled / total).fillna(0).rename(f"cancellation_rate_{window_days}d")
        features[f"cancellation_rate_{window_days}d"] = cancel_rate

    trajectory = pd.DataFrame(features)
    logger.info("  → Trajectory features: %d columns for %d accounts", trajectory.shape[1], trajectory.shape[0])
    return trajectory


# =============================================================================
# Feature Group 3: Service Adoption
# =============================================================================

def compute_service_features(contracts: pd.DataFrame) -> pd.DataFrame:
    """Compute service adoption features from contract data."""
    logger.info("Computing service adoption features...")

    # Only consider contracts active as of cutoff
    active = contracts[(contracts["start_date"] <= CUTOFF_DATE) & (contracts["end_date"] >= CUTOFF_DATE)]

    features = {}

    # Number of active products
    features["num_active_products"] = active.groupby("account_id")["product"].nunique().rename("num_active_products")

    # Product diversity score (normalized entropy)
    # Max entropy = log2(4) for 4 products
    max_entropy = np.log2(4)

    def _product_diversity(group):
        p = group["product"].value_counts(normalize=True).values
        entropy = -np.sum(p * np.log2(p + 1e-10))
        return entropy / max_entropy if max_entropy > 0 else 0

    diversity = active.groupby("account_id").apply(_product_diversity, include_groups=False).rename("product_diversity_score")
    features["product_diversity_score"] = diversity

    # Total active contract value
    features["active_contract_value"] = active.groupby("account_id")["contract_value"].sum().rename("active_contract_value")

    # Contract renewal count (contracts that started after a previous one ended for same product)
    all_contracts = contracts.sort_values(["account_id", "product", "start_date"])

    def _count_renewals(group):
        if len(group) < 2:
            return 0
        renewals = 0
        prev_end = group.iloc[0]["end_date"]
        for _, row in group.iloc[1:].iterrows():
            # Consider it a renewal if new contract starts within 90 days of previous end
            if (row["start_date"] - prev_end).days <= 90:
                renewals += 1
            prev_end = row["end_date"]
        return renewals

    renewal_counts = all_contracts.groupby(["account_id", "product"]).apply(
        _count_renewals, include_groups=False
    ).groupby("account_id").sum().rename("contract_renewal_count")
    features["contract_renewal_count"] = renewal_counts

    # Specific product adoption flags (binary — for cross-sell targeting)
    for product in ["Neo", "Egencia Analytics Studio", "Meetings & Events", "Travel Consulting"]:
        col_name = f"has_{product.lower().replace(' ', '_').replace('&', 'and')}"
        has_product = active[active["product"] == product].groupby("account_id").size().clip(upper=1).rename(col_name)
        features[col_name] = has_product

    service = pd.DataFrame(features)
    logger.info("  → Service features: %d columns for %d accounts", service.shape[1], service.shape[0])
    return service


# =============================================================================
# Feature Group 4: Support Health
# =============================================================================

def compute_support_features(tickets: pd.DataFrame) -> pd.DataFrame:
    """Compute support health features from ticket data."""
    logger.info("Computing support health features...")

    hist = tickets[tickets["created_date"] < CUTOFF_DATE].copy()

    features = {}

    # Overall metrics
    features["total_ticket_count"] = hist.groupby("account_id").size().rename("total_ticket_count")
    features["avg_resolution_hours"] = hist.groupby("account_id")["resolution_hours"].mean().rename("avg_resolution_hours")

    # Ticket rate per month (normalized by account tenure in the data)
    first_ticket = hist.groupby("account_id")["created_date"].min()
    last_ticket = hist.groupby("account_id")["created_date"].max()
    ticket_span_months = ((last_ticket - first_ticket).dt.days / 30.44).clip(lower=1)
    ticket_count = hist.groupby("account_id").size()
    features["ticket_rate_per_month"] = (ticket_count / ticket_span_months).rename("ticket_rate_per_month")

    # P1/P2 escalation ratio
    severe = hist[hist["severity"].isin(["P1", "P2"])].groupby("account_id").size()
    total = hist.groupby("account_id").size()
    features["p1_p2_escalation_ratio"] = (severe / total).fillna(0).rename("p1_p2_escalation_ratio")

    # Recent ticket rate (last 90 days vs overall)
    recent_cutoff = CUTOFF_DATE - pd.Timedelta(days=90)
    recent_tickets = hist[hist["created_date"] >= recent_cutoff].groupby("account_id").size()
    features["ticket_count_90d"] = recent_tickets.rename("ticket_count_90d")

    # Ticket trend: ratio of recent (90d) rate to overall rate
    overall_monthly = features["ticket_rate_per_month"]
    recent_monthly = (recent_tickets / 3).fillna(0)  # 3 months
    features["ticket_trend_ratio"] = (recent_monthly / overall_monthly.clip(lower=0.01)).rename("ticket_trend_ratio")

    # Average resolution time for severe tickets
    severe_tickets = hist[hist["severity"].isin(["P1", "P2"])]
    features["avg_severe_resolution_hours"] = severe_tickets.groupby("account_id")["resolution_hours"].mean().rename("avg_severe_resolution_hours")

    # Category diversity (are issues spread across categories or concentrated?)
    def _category_concentration(group):
        p = group["category"].value_counts(normalize=True).values
        return p.max()  # Herfindahl-style: 1.0 = all same category

    features["ticket_category_concentration"] = hist.groupby("account_id").apply(
        _category_concentration, include_groups=False
    ).rename("ticket_category_concentration")

    support = pd.DataFrame(features)
    logger.info("  → Support features: %d columns for %d accounts", support.shape[1], support.shape[0])
    return support


# =============================================================================
# Feature Group 5: Policy Compliance
# =============================================================================

def compute_policy_features(bookings: pd.DataFrame, travelers: pd.DataFrame) -> pd.DataFrame:
    """Compute out-of-policy booking rates and trends."""
    logger.info("Computing policy compliance features...")

    traveler_to_account = travelers.set_index("traveler_id")["account_id"]
    bookings = bookings.copy()
    bookings["account_id"] = bookings["traveler_id"].map(traveler_to_account)

    hist = bookings[bookings["booking_date"] < CUTOFF_DATE].copy()

    features = {}

    for window_days in [30, 90]:
        window_start = CUTOFF_DATE - pd.Timedelta(days=window_days)
        windowed = hist[hist["booking_date"] >= window_start]
        total = windowed.groupby("account_id").size()
        oop = windowed[windowed["is_out_of_policy"]].groupby("account_id").size()
        features[f"out_of_policy_rate_{window_days}d"] = (oop / total).fillna(0).rename(f"out_of_policy_rate_{window_days}d")

    # OOP trend: 30d rate vs 90d rate
    if "out_of_policy_rate_30d" in features and "out_of_policy_rate_90d" in features:
        features["oop_trend"] = (features["out_of_policy_rate_30d"] - features["out_of_policy_rate_90d"]).rename("oop_trend")

    # Destination diversity (proxy for travel program complexity)
    dest_nunique = hist.groupby("account_id")["destination_region"].nunique().rename("destination_diversity")
    features["destination_diversity"] = dest_nunique

    # Average lead time (booking to travel)
    hist["lead_time"] = (hist["travel_date"] - hist["booking_date"]).dt.days
    features["avg_lead_time_days"] = hist.groupby("account_id")["lead_time"].mean().rename("avg_lead_time_days")

    policy = pd.DataFrame(features)
    logger.info("  → Policy features: %d columns for %d accounts", policy.shape[1], policy.shape[0])
    return policy


# =============================================================================
# Feature Assembly
# =============================================================================

def assemble_features(
    accounts: pd.DataFrame,
    rfm: pd.DataFrame,
    trajectory: pd.DataFrame,
    service: pd.DataFrame,
    support: pd.DataFrame,
    policy: pd.DataFrame,
    clv_labels: pd.DataFrame,
) -> pd.DataFrame:
    """Join all feature groups into a single account-level feature matrix."""
    logger.info("Assembling final feature matrix...")

    # Start with account base attributes
    base = accounts[["account_id", "tier", "region", "industry", "is_churned", "annual_contract_value"]].copy()
    base["onboarding_date"] = pd.to_datetime(accounts["onboarding_date"])
    base["tenure_days"] = (CUTOFF_DATE - base["onboarding_date"]).dt.days.clip(lower=0)

    # Encode tier as ordinal
    tier_map = {"Platinum": 4, "Gold": 3, "Silver": 2, "Bronze": 1}
    base["tier_rank"] = base["tier"].map(tier_map)

    # Join all feature groups
    base = base.set_index("account_id")
    for feature_df in [rfm, trajectory, service, support, policy]:
        base = base.join(feature_df, how="left")

    # Join CLV labels
    clv = clv_labels.set_index("account_id")
    base = base.join(clv[["clv_12m"]], how="left")

    # Fill NaN with 0 for numerical features (accounts with no activity in that window)
    numeric_cols = base.select_dtypes(include=[np.number]).columns
    base[numeric_cols] = base[numeric_cols].fillna(0)

    # Add feature timestamp
    base["feature_timestamp"] = pd.Timestamp.now()

    base = base.reset_index()

    logger.info("  → Final feature matrix: %d accounts × %d features", base.shape[0], base.shape[1])
    logger.info("  → Numeric features: %d", len(numeric_cols))
    logger.info("  → Null counts:\n%s", base.isnull().sum()[base.isnull().sum() > 0].to_string())

    return base


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Compute ML features from synthetic data")
    parser.add_argument("--data-dir", type=str, default="data/synthetic", help="Input data directory")
    parser.add_argument("--output-dir", type=str, default="data/features", help="Output directory for feature matrix")
    parser.add_argument("--format", choices=["csv", "parquet", "both"], default="both", help="Output format")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Feature Engineering Pipeline")
    logger.info("=" * 60)
    logger.info("Cutoff date: %s", CUTOFF_DATE.strftime("%Y-%m-%d"))
    logger.info("Windows: %s days", WINDOWS)

    # Load data
    tables = load_data(data_dir)

    # Compute feature groups
    rfm = compute_rfm_features(tables["bookings"], tables["traveler_profiles"])
    trajectory = compute_trajectory_features(tables["bookings"], tables["traveler_profiles"])
    service = compute_service_features(tables["service_contracts"])
    support = compute_support_features(tables["support_tickets"])
    policy = compute_policy_features(tables["bookings"], tables["traveler_profiles"])

    # Assemble
    feature_matrix = assemble_features(
        accounts=tables["corporate_accounts"],
        rfm=rfm,
        trajectory=trajectory,
        service=service,
        support=support,
        policy=policy,
        clv_labels=tables["clv_labels"],
    )

    # Save
    if args.format in ("csv", "both"):
        csv_path = output_dir / "account_features.csv"
        feature_matrix.to_csv(csv_path, index=False)
        logger.info("Saved → %s", csv_path)

    if args.format in ("parquet", "both"):
        parquet_path = output_dir / "account_features.parquet"
        feature_matrix.to_parquet(parquet_path, index=False, engine="pyarrow")
        logger.info("Saved → %s", parquet_path)

    # Feature summary
    logger.info("=" * 60)
    logger.info("FEATURE ENGINEERING COMPLETE")
    logger.info("=" * 60)
    logger.info("Shape: %d accounts × %d features", feature_matrix.shape[0], feature_matrix.shape[1])

    # Print feature statistics
    numeric = feature_matrix.select_dtypes(include=[np.number])
    stats = numeric.describe().T[["mean", "std", "min", "max"]]
    logger.info("\nFeature Statistics:\n%s", stats.to_string())


if __name__ == "__main__":
    main()
