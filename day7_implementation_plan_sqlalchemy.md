# Snowflake SQLAlchemy Migration Plan

The latest execution triggered a `UserWarning` from Pandas because we are passing a raw Snowflake DBAPI2 connection. This plan transitions our data pipelines to use **SQLAlchemy**, which is the recommended and fully-tested interface for Pandas 2.1+.

## Proposed Changes

We will refactor our Snowflake utilities to use a SQLAlchemy `Engine` instead of a raw `Connection`.

### [Core]

#### [MODIFY] [requirements.txt](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/requirements.txt)
*   Explicitly add `sqlalchemy>=2.0` to the Data Infrastructure section.

### [Data Infrastructure]

#### [MODIFY] [snowflake_loader.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/data/snowflake_loader.py)
*   **Engine Creation**: Update `get_connection()` to `get_snowflake_engine()`. This will use `sqlalchemy.create_engine()` with the `snowflake://` dialect.
*   **DDL Execution**: Use `engine.connect()` and `sqlalchemy.text()` for executing the DDL string.
*   **Data Ingestion**: Update `PUT` and `COPY INTO` commands to run via the SQLAlchemy connection context.

#### [MODIFY] [feature_engineering.py](file:///d:/Downloads/AmericanExpressProjects/agentic_ai/customer_lifetime_value_and_cross_sell_predictor/features/feature_engineering.py)
*   **Engine Creation**: Update `get_snowflake_connection()` to `get_snowflake_engine()`.
*   **Data Loading**: In `load_data_snowflake()`, replace `pd.read_sql(query, conn)` with `pd.read_sql(query, engine)`. This will remove the `UserWarning`.

## Verification Plan

### Automated Tests
*   Run the Data Loader: `.venv\Scripts\python -m data.snowflake_loader --data-dir data/synthetic`
*   Run Feature Engineering: `.venv\Scripts\python -m features.feature_engineering --source snowflake`
*   **Success Criteria**: Both scripts finish with "COMPLETE" status and **zero UserWarnings** from Pandas regarding the connection type.

### Manual Verification
*   Check that `data/features/account_features.parquet` is overwritten with valid data.
*   Verify the Snowflake console shows active sessions using the "SQLAlchemy" application tag.
