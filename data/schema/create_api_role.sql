-- =============================================================================
-- SECURITY: Least-Privilege Snowflake Role for API (VULN-015)
-- =============================================================================
-- Run this script once as ACCOUNTADMIN to create a read-only role for the
-- inference API. This replaces the overly-broad SYSADMIN role that was
-- previously used in the application.
--
-- Usage:
--   snowsql -a <account> -u <admin_user> -f data/schema/create_api_role.sql
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Create the read-only role
CREATE ROLE IF NOT EXISTS CLV_API_READER
    COMMENT = 'Read-only role for CLV inference API — least privilege (VULN-015)';

-- 2. Grant warehouse usage (compute only, no admin)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE CLV_API_READER;

-- 3. Grant read-only access to the database and required schemas
GRANT USAGE ON DATABASE CLV_CROSS_SELL TO ROLE CLV_API_READER;
GRANT USAGE ON SCHEMA CLV_CROSS_SELL.RAW TO ROLE CLV_API_READER;
GRANT USAGE ON SCHEMA CLV_CROSS_SELL.STAGING TO ROLE CLV_API_READER;
GRANT USAGE ON SCHEMA CLV_CROSS_SELL.FEATURES TO ROLE CLV_API_READER;

-- 4. Grant SELECT on all current and future tables in relevant schemas
GRANT SELECT ON ALL TABLES IN SCHEMA CLV_CROSS_SELL.RAW TO ROLE CLV_API_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA CLV_CROSS_SELL.STAGING TO ROLE CLV_API_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA CLV_CROSS_SELL.FEATURES TO ROLE CLV_API_READER;

GRANT SELECT ON FUTURE TABLES IN SCHEMA CLV_CROSS_SELL.RAW TO ROLE CLV_API_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CLV_CROSS_SELL.STAGING TO ROLE CLV_API_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CLV_CROSS_SELL.FEATURES TO ROLE CLV_API_READER;

-- 5. Assign the role to the API service user
-- Replace ADDY9051 with your actual service account username
GRANT ROLE CLV_API_READER TO USER ADDY9051;

-- 6. Set as default role for the service user
ALTER USER ADDY9051 SET DEFAULT_ROLE = CLV_API_READER;

-- Verification
SHOW GRANTS TO ROLE CLV_API_READER;
