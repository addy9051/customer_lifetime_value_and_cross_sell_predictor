{{ config(materialized='table') }}

with churn as (
    select * from {{ ref('stg_churn_predictions') }}
)

select
    account_id,
    prediction_date,
    churn_risk_score,
    case
        when churn_risk_score > 0.5 then 'High'
        when churn_risk_score > 0.25 then 'Medium'
        else 'Low'
    end as risk_level,
    survival_prob_30d,
    survival_prob_90d,
    survival_prob_180d,
    survival_prob_365d,
    expected_lifetime_days
from churn
