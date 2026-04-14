-- =============================================================================
-- Snowflake DDL: CLV & Cross-Sell Predictor
-- =============================================================================
-- Creates the database, warehouse, and three schemas:
--   RAW       → raw ingested data from synthetic generator
--   STAGING   → cleansed and typed data
--   FEATURES  → computed ML feature tables
-- =============================================================================

-- -------------------------------------------------------
-- Database & Warehouse
-- -------------------------------------------------------
CREATE DATABASE IF NOT EXISTS CLV_CROSS_SELL;
USE DATABASE CLV_CROSS_SELL;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE;

USE WAREHOUSE COMPUTE_WH;

-- -------------------------------------------------------
-- RAW Schema — mirrors CSV structure exactly
-- -------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS RAW;

CREATE OR REPLACE TABLE RAW.CORPORATE_ACCOUNTS (
    account_id              VARCHAR(10)     PRIMARY KEY,
    company_name            VARCHAR(200),
    industry                VARCHAR(100),
    region                  VARCHAR(50),
    tier                    VARCHAR(20),
    onboarding_date         DATE,
    is_churned              BOOLEAN,
    churn_date              DATE,
    annual_contract_value   NUMBER(12,2)
);

CREATE OR REPLACE TABLE RAW.SERVICE_CONTRACTS (
    contract_id     VARCHAR(12)     PRIMARY KEY,
    account_id      VARCHAR(10)     REFERENCES RAW.CORPORATE_ACCOUNTS(account_id),
    product         VARCHAR(50),
    start_date      DATE,
    end_date        DATE,
    contract_value  NUMBER(12,2),
    is_active       BOOLEAN
);

CREATE OR REPLACE TABLE RAW.TRAVELER_PROFILES (
    traveler_id     VARCHAR(12)     PRIMARY KEY,
    account_id      VARCHAR(10)     REFERENCES RAW.CORPORATE_ACCOUNTS(account_id),
    role            VARCHAR(50),
    travel_tier     VARCHAR(20)
);

CREATE OR REPLACE TABLE RAW.BOOKINGS (
    booking_id          VARCHAR(14)     PRIMARY KEY,
    traveler_id         VARCHAR(12)     REFERENCES RAW.TRAVELER_PROFILES(traveler_id),
    booking_type        VARCHAR(10),
    booking_date        DATE,
    travel_date         DATE,
    amount              NUMBER(10,2),
    is_out_of_policy    BOOLEAN,
    is_cancelled        BOOLEAN,
    destination_region  VARCHAR(50)
);

CREATE OR REPLACE TABLE RAW.SUPPORT_TICKETS (
    ticket_id           VARCHAR(12)     PRIMARY KEY,
    account_id          VARCHAR(10)     REFERENCES RAW.CORPORATE_ACCOUNTS(account_id),
    created_date        TIMESTAMP,
    resolved_date       TIMESTAMP,
    severity            VARCHAR(5),
    category            VARCHAR(20),
    resolution_hours    NUMBER(8,2)
);

-- -------------------------------------------------------
-- STAGING Schema — cleansed, with derived helper columns
-- -------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS STAGING;

CREATE OR REPLACE TABLE STAGING.CORPORATE_ACCOUNTS AS
SELECT
    account_id,
    company_name,
    industry,
    region,
    tier,
    onboarding_date,
    is_churned,
    churn_date,
    annual_contract_value,
    DATEDIFF('day', onboarding_date, COALESCE(churn_date, CURRENT_DATE())) AS tenure_days,
    CASE
        WHEN tier = 'Platinum' THEN 4
        WHEN tier = 'Gold'     THEN 3
        WHEN tier = 'Silver'   THEN 2
        ELSE 1
    END AS tier_rank
FROM RAW.CORPORATE_ACCOUNTS;

CREATE OR REPLACE TABLE STAGING.SERVICE_CONTRACTS AS
SELECT
    contract_id,
    account_id,
    product,
    start_date,
    end_date,
    contract_value,
    is_active,
    DATEDIFF('month', start_date, end_date) AS contract_months
FROM RAW.SERVICE_CONTRACTS;

CREATE OR REPLACE TABLE STAGING.TRAVELER_PROFILES AS
SELECT * FROM RAW.TRAVELER_PROFILES;

CREATE OR REPLACE TABLE STAGING.BOOKINGS AS
SELECT
    b.booking_id,
    b.traveler_id,
    t.account_id,
    b.booking_type,
    b.booking_date,
    b.travel_date,
    b.amount,
    b.is_out_of_policy,
    b.is_cancelled,
    b.destination_region,
    DATEDIFF('day', b.booking_date, b.travel_date) AS lead_time_days
FROM RAW.BOOKINGS b
JOIN RAW.TRAVELER_PROFILES t ON b.traveler_id = t.traveler_id;

CREATE OR REPLACE TABLE STAGING.SUPPORT_TICKETS AS
SELECT
    ticket_id,
    account_id,
    created_date,
    resolved_date,
    severity,
    category,
    resolution_hours,
    CASE severity
        WHEN 'P1' THEN 2000
        WHEN 'P2' THEN 500
        WHEN 'P3' THEN 150
        WHEN 'P4' THEN 50
    END AS cost_proxy
FROM RAW.SUPPORT_TICKETS;

-- -------------------------------------------------------
-- FEATURES Schema — ML feature tables (populated by
-- Databricks / Airflow feature engineering pipeline)
-- -------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS FEATURES;

-- This table will be populated by the feature engineering DAG
CREATE OR REPLACE TABLE FEATURES.ACCOUNT_FEATURES (
    account_id                  VARCHAR(10)     PRIMARY KEY,
    -- RFM Features
    days_since_last_booking     INTEGER,
    booking_count_30d           INTEGER,
    booking_count_90d           INTEGER,
    booking_count_180d          INTEGER,
    total_spend_30d             NUMBER(12,2),
    total_spend_90d             NUMBER(12,2),
    total_spend_180d            NUMBER(12,2),
    -- Behavioral Trajectory
    booking_volume_trend        FLOAT,          -- slope of monthly bookings
    spend_acceleration          FLOAT,          -- 2nd derivative of spend
    cancellation_rate_30d       FLOAT,
    cancellation_rate_90d       FLOAT,
    -- Service Adoption
    num_active_products         INTEGER,
    product_diversity_score     FLOAT,          -- normalized entropy
    contract_renewal_count      INTEGER,
    -- Support Health
    ticket_rate_per_month       FLOAT,
    avg_resolution_hours        FLOAT,
    p1_p2_escalation_ratio      FLOAT,
    -- Policy Compliance
    out_of_policy_rate_30d      FLOAT,
    out_of_policy_rate_90d      FLOAT,
    -- Meta
    tier                        VARCHAR(20),
    tenure_days                 INTEGER,
    is_churned                  BOOLEAN,
    annual_contract_value       NUMBER(12,2),
    -- Labels (for training)
    clv_12m                     NUMBER(12,2),
    feature_timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- CLV Labels (from the synthetic generator)
CREATE OR REPLACE TABLE FEATURES.CLV_LABELS (
    account_id              VARCHAR(10)     PRIMARY KEY,
    booking_revenue_12m     NUMBER(12,2),
    contract_value_active   NUMBER(12,2),
    support_cost_12m        NUMBER(12,2),
    clv_12m                 NUMBER(12,2)
);
