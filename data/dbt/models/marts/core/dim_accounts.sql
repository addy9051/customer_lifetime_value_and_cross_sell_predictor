{{ config(materialized='table') }}

with accounts as (
    select * from {{ ref('stg_corporate_accounts') }}
)

select
    account_id,
    company_name,
    coalesce(nullif(industry, ''), 'Not Specified') as industry,
    coalesce(nullif(region, ''), 'Not Specified') as region,
    coalesce(nullif(tier, ''), 'Uncategorized') as tier,
    onboarding_date,
    coalesce(churn_date, '9999-12-31') as churn_date,
    is_churned,
    tenure_days,
    annual_contract_value as base_acv
from accounts
