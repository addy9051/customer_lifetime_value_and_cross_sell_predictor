{{ config(materialized='table') }}

with accounts as (
    select * from {{ ref('stg_corporate_accounts') }}
)

select
    account_id,
    company_name,
    industry,
    region,
    tier,
    onboarding_date,
    churn_date,
    is_churned,
    tenure_days,
    annual_contract_value as base_acv
from accounts
