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
    from pyspark.sql.window import Window
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Cutoff date aligns with our local Pandas script
CUTOFF_DATE = "2023-01-01"


def get_spark_session():
    """Initialize a Spark Session."""
    return SparkSession.builder \
        .appName("AmexGBT-FeatureEngineering") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()


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
    """Compute Recency, Frequency, and Monetary features using PySpark SQL."""
    logger.info("Computing scalable RFM features...")
    
    # Filter bookings prior to cutoff
    past_bookings = bookings_df.filter(F.col("booking_date") < F.lit(cutoff_date).cast("timestamp"))
    
    # Base aggregation metrics
    rfm = past_bookings.groupBy("account_id").agg(
        F.count("*").alias("total_ticket_count"),
        F.sum("total_amount").alias("total_spend"),
        F.max("booking_date").alias("last_booking_date")
    ).withColumn(
        "days_since_last_booking",
        F.datediff(F.lit(cutoff_date).cast("timestamp"), F.col("last_booking_date"))
    )

    # Time-window aggregations (30, 90, 180 days)
    for window_days in [30, 90, 180]:
        window_start = F.date_sub(F.lit(cutoff_date).cast("timestamp"), window_days)
        
        window_df = past_bookings.filter(F.col("booking_date") >= window_start) \
            .groupBy("account_id").agg(
                F.count("*").alias(f"booking_count_{window_days}d"),
                F.sum("total_amount").alias(f"total_spend_{window_days}d")
            )
        
        rfm = rfm.join(window_df, on="account_id", how="left")
        
        # Calculate AOV (Fill nulls with 0)
        rfm = rfm.withColumn(
            f"aov_{window_days}d", 
            F.when(F.col(f"booking_count_{window_days}d") > 0, 
                   F.col(f"total_spend_{window_days}d") / F.col(f"booking_count_{window_days}d"))
             .otherwise(0.0)
        )

    # Drop intermediate dates, clean up nulls
    rfm = rfm.drop("last_booking_date").fillna(0.0)
    
    # Join back to accounts
    return accounts_df.join(rfm, on="account_id", how="left").fillna(0.0)


def compute_service_adoption(spark, accounts_df, contracts_df, cutoff_date: str):
    """Compute product usage features in PySpark."""
    logger.info("Computing multi-product adoption flags...")
    
    # Active contracts at cutoff
    active = contracts_df.filter(
        (F.col("start_date") < F.lit(cutoff_date).cast("timestamp")) & 
        (F.col("end_date") > F.lit(cutoff_date).cast("timestamp"))
    )
    
    # Aggregate to account level
    adoption = active.groupBy("account_id").agg(
        F.count("*").alias("num_active_products"),
        F.sum("annual_value").alias("active_contract_value"),
        # Create boolean adoption columns using pivot-like operations
        F.max(F.when(F.col("product_line") == "Neo", 1).otherwise(0)).alias("has_neo"),
        F.max(F.when(F.col("product_line") == "Egencia Analytics Studio", 1).otherwise(0)).alias("has_egencia_analytics_studio"),
        F.max(F.when(F.col("product_line") == "Meetings & Events", 1).otherwise(0)).alias("has_meetings_and_events"),
        F.max(F.when(F.col("product_line") == "Travel Consulting", 1).otherwise(0)).alias("has_travel_consulting")
    )
    
    return accounts_df.join(adoption, on="account_id", how="left").fillna(0.0)


def main():
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
        accounts, profiles, bookings, contracts, tickets, clv = load_tables(spark, data_dir)
        
        # 2. RFM Features
        feature_matrix = compute_rfm_features(spark, accounts, bookings, CUTOFF_DATE)
        
        # 3. Service Adoption
        feature_matrix = compute_service_adoption(spark, feature_matrix, contracts, CUTOFF_DATE)

        # 4. Join Labels (If Available)
        if clv is not None:
            feature_matrix = feature_matrix.join(
                clv.select("account_id", "clv_12m"), on="account_id", how="left"
            )

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
