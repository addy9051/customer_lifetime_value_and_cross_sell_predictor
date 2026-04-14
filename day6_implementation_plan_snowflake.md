# Production Data Integration: Snowflake

This plan outlines the steps to disconnect the PySpark/Pandas Feature Engineering pipeline from local/synthetic CSVs and connect it directly to the production Snowflake data warehouse.

## Proposed Changes

We will modify `features/feature_engineering.py` to seamlessly toggle between taking data from a local directory (for testing/development) and fetching it securely from Snowflake (for production).

### [features]

#### [MODIFY] [feature_engineering.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/features/feature_engineering.py)
*   **CLI Arguments**: Introduce `--source` (choices: `local`, `snowflake`) and `--snowflake-schema` (default: `STAGING`).
*   **Snowflake Loader Function**: Create a new `load_data_snowflake()` function that:
    1. Authenticates using environment variables (the same way the `deploy.yml` pipeline sets up the Kubernetes Pod).
    2. Downloads `CORPORATE_ACCOUNTS`, `TRAVELER_PROFILES`, `BOOKINGS`, `SERVICE_CONTRACTS`, and `SUPPORT_TICKETS` directly from Snowflake STAGING.
    3. Fetches `CLV_LABELS` from the Snowflake `FEATURES` schema.
    4. Automatically normalizes column names from UPPERCASE (Snowflake default) to lowercase so the remainder of the pipeline remains unchanged.
*   **Main Logic**: Route the execution to either `load_data()` or `load_data_snowflake()` based on the `--source` argument.

#### [MODIFY] [feature_engineering_pyspark.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/features/feature_engineering_pyspark.py) (Optional)
If PySpark is the primary execution engine in Databricks, we should verify that this pipeline is also aware of the Snowflake JDBC connectors. However, for the immediate Airflow/container workflow, integrating the core `feature_engineering.py` addresses the immediate dependency.

## Verification Plan

### Automated Tests
*   Run the script locally passing the flag `--source snowflake`.
*   Observe the logging output to verify that it skips reading local Parquet/CSV files and correctly connects to the `HVNUNCL-JIC27363` Snowflake account.

### Manual Verification
*   Verify that the resulting `account_features.parquet` matrix maintains 49 features and non-null values after pivoting to the live data source.
