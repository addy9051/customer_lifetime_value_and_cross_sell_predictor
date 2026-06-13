"""
PySpark Feature Engineering Pipeline (Databricks Target)
======================================================
Distributed PySpark implementation of the CLV & Cross-Sell feature engineering pipeline.
Designed to run on Databricks clusters for processing data lakes with hundreds of millions of rows.

Usage (Local Test):
    pip install pyspark
    python -m features.feature_engineering_pyspark
"""

import argparse
import logging
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Cutoff date aligns with our local Pandas script
CUTOFF_DATE = "2023-01-01"


def get_spark_session():
    """Initialize a Spark Session."""
    return (
        SparkSession.builder.appName("AmexGBT-FeatureEngineering")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def load_tables(spark, data_dir: Path):
    """Load the synthetic raw CSV data into Spark DataFrames."""
    logger.info("Loading Spark DataFrames from %s...", data_dir)

    # In production, these paths would likely be dbfs:/ or s3:// paths
    accounts_df = spark.read.csv(str(data_dir / "corporate_accounts.csv"), header=True, inferSchema=True)
    profiles_df = spark.read.csv(str(data_dir / "traveler_profiles.csv"), header=True, inferSchema=True)
    bookings_df = spark.read.csv(str(data_dir / "bookings.csv"), header=True, inferSchema=True)
    contracts_df = spark.read.csv(str(data_dir / "service_contracts.csv"), header=True, inferSchema=True)
    tickets_df = spark.read.csv(str(data_dir / "support_tickets.csv"), header=True, inferSchema=True)

    clv_path = data_dir / "clv_labels.csv"
    clv_df = None
    if clv_path.exists():
        clv_df = spark.read.csv(str(clv_path), header=True, inferSchema=True)

    logger.info("Loaded Accounts: %d rows", accounts_df.count())
    return accounts_df, profiles_df, bookings_df, contracts_df, tickets_df, clv_df


def compute_rfm_features(spark, accounts_df, bookings_df, cutoff_date: str):
    """
    Compute account-level Recency, Frequency, and Monetary (RFM) features as of the provided cutoff date and attach them to the accounts DataFrame.
    
    Parameters:
        spark: SparkSession instance used for Spark operations.
        accounts_df (DataFrame): Account-level DataFrame containing `account_id`.
        bookings_df (DataFrame): Bookings DataFrame with at least `account_id`, `booking_date`, and `amount`.
        cutoff_date (str): Cutoff date (ISO string) to compute features as of this date.
    
    Returns:
        DataFrame: `accounts_df` left-joined with per-account RFM features including:
            - total_booking_count, total_spend, days_since_last_booking
            - booking_count_30d, total_spend_30d, aov_30d
            - booking_count_90d, total_spend_90d, aov_90d
            - booking_count_180d, total_spend_180d, aov_180d
        All feature columns use numeric types and missing values are filled with 0.0.
    """
    logger.info("Computing scalable RFM features...")

    # Filter bookings prior to cutoff
    past_bookings = bookings_df.filter(F.col("booking_date") < F.lit(cutoff_date).cast("timestamp"))

    # Base aggregation metrics
    rfm = (
        past_bookings.groupBy("account_id")
        .agg(
            F.count("*").alias("total_booking_count"),
            F.sum("amount").alias("total_spend"),
            F.max("booking_date").alias("last_booking_date"),
        )
        .withColumn(
            "days_since_last_booking", F.datediff(F.lit(cutoff_date).cast("timestamp"), F.col("last_booking_date"))
        )
    )

    # Time-window aggregations (30, 90, 180 days)
    for window_days in [30, 90, 180]:
        window_start = F.date_sub(F.lit(cutoff_date).cast("timestamp"), window_days)

        window_df = (
            past_bookings.filter(F.col("booking_date") >= window_start)
            .groupBy("account_id")
            .agg(
                F.count("*").alias(f"booking_count_{window_days}d"),
                F.sum("amount").alias(f"total_spend_{window_days}d"),
            )
        )

        rfm = rfm.join(window_df, on="account_id", how="left")

        # Calculate AOV (Fill nulls with 0)
        rfm = rfm.withColumn(
            f"aov_{window_days}d",
            F.when(
                F.col(f"booking_count_{window_days}d") > 0,
                F.col(f"total_spend_{window_days}d") / F.col(f"booking_count_{window_days}d"),
            ).otherwise(0.0),
        )

    # Drop intermediate dates, clean up nulls
    rfm = rfm.drop("last_booking_date").fillna(0.0)

    # Join back to accounts
    return accounts_df.join(rfm, on="account_id", how="left").fillna(0.0)


def compute_service_adoption(spark, accounts_df, contracts_df, cutoff_date: str):
    """
    Compute account-level multi-product adoption flags and aggregates as of a cutoff date.
    
    Filters contracts that are active at the cutoff (keeps rows where start_date < cutoff_date and end_date > cutoff_date), aggregates per account to produce:
    - num_active_products: count of distinct active products
    - active_contract_value: sum of contract_value for active contracts
    - has_neo, has_egencia_analytics_studio, has_meetings_and_events, has_travel_consulting: binary (0/1) flags indicating presence of those products
    
    Parameters:
        spark: SparkSession (unused directly but kept for API consistency).
        accounts_df: DataFrame containing account-level rows keyed by `account_id`.
        contracts_df: DataFrame of service contracts with `account_id`, `product`, `contract_value`, `start_date`, and `end_date`.
        cutoff_date (str): Cutoff timestamp (string) used to determine active contracts.
    
    Returns:
        DataFrame: `accounts_df` left-joined with the adoption aggregates and flags; missing values are filled with 0.0.
    """
    logger.info("Computing multi-product adoption flags...")

    # Active contracts at cutoff
    active = contracts_df.filter(
        (F.col("start_date") < F.lit(cutoff_date).cast("timestamp"))
        & (F.col("end_date") > F.lit(cutoff_date).cast("timestamp"))
    )

    # Aggregate to account level
    adoption = active.groupBy("account_id").agg(
        F.countDistinct("product").alias("num_active_products"),
        F.sum("contract_value").alias("active_contract_value"),
        # Create boolean adoption columns using pivot-like operations
        F.max(F.when(F.col("product") == "Neo", 1).otherwise(0)).alias("has_neo"),
        F.max(F.when(F.col("product") == "Egencia Analytics Studio", 1).otherwise(0)).alias(
            "has_egencia_analytics_studio"
        ),
        F.max(F.when(F.col("product") == "Meetings & Events", 1).otherwise(0)).alias("has_meetings_and_events"),
        F.max(F.when(F.col("product") == "Travel Consulting", 1).otherwise(0)).alias("has_travel_consulting"),
    )

    return accounts_df.join(adoption, on="account_id", how="left").fillna(0.0)


def main():
    """
    Run the PySpark feature engineering pipeline: load input tables, derive account-level RFM and service-adoption features as of the module cutoff date, optionally join CLV labels, and write the resulting account feature matrix to Parquet.
    
    The function is a CLI entry point that accepts `--data-dir` (default: "data/synthetic") and `--output-dir` (default: "data/features_spark"). It requires PySpark to be available; if not present, the function logs an error and exits. When executed it creates a Spark session, ensures the output directory exists, loads CSV input tables, computes RFM and product-adoption features aligned to the module cutoff date, joins 12-month CLV labels when available, writes the final feature matrix to `<output-dir>/account_features_spark` in Parquet format (coalesced to a single file for local emulation), and always stops the Spark session on exit.
    """
    parser = argparse.ArgumentParser(description="PySpark Feature Engineering")
    parser.add_argument("--data-dir", type=str, default="data/synthetic")
    parser.add_argument("--output-dir", type=str, default="data/features_spark")
    args = parser.parse_args()

    if not HAS_SPARK:
        logger.error("PySpark is not installed! Run: pip install pyspark")
        return

    logger.info("=" * 60)
    logger.info("PySpark Feature Engineering Pipeline")
    logger.info("=" * 60)

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session()

    try:
        # 1. Load Data
        accounts, _profiles, bookings, contracts, _tickets, clv = load_tables(spark, data_dir)

        # 2. RFM Features
        feature_matrix = compute_rfm_features(spark, accounts, bookings, CUTOFF_DATE)

        # 3. Service Adoption
        feature_matrix = compute_service_adoption(spark, feature_matrix, contracts, CUTOFF_DATE)

        # 4. Join Labels (If Available)
        if clv is not None:
            feature_matrix = feature_matrix.join(clv.select("account_id", "clv_12m"), on="account_id", how="left")

        # 5. Output
        logger.info("Writing distributed Parquet to %s...", output_dir)

        # In a real cluster we'd write partitioned parquet.
        # Coalescing to 1 here just for local emulation.
        feature_matrix.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "account_features_spark"))

        logger.info("✅ PySpark Execution Complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
