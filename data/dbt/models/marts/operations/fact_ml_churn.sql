{{ config(materialized='table') }}

with churn as (
    select * from {{ ref('stg_churn_predictions') }}
)

, account_renewals as (
    select 
        account_id,
        max(end_date) as max_end_date
    from {{ ref('stg_service_contracts') }}
    group by 1
)

select
    c.account_id,
    c.prediction_date,
    coalesce(c.churn_risk_score, 0) as churn_risk_score,
    case
        when c.churn_risk_score > 0.5 then 'High'
        when c.churn_risk_score > 0.25 then 'Medium'
        else 'Low'
    end as risk_level,
    coalesce(c.survival_prob_30d, 1) as survival_prob_30d,
    coalesce(c.survival_prob_90d, 1) as survival_prob_90d,
    coalesce(c.survival_prob_180d, 1) as survival_prob_180d,
    coalesce(c.survival_prob_365d, 1) as survival_prob_365d,
    coalesce(c.expected_lifetime_days, 3650) as expected_lifetime_days,
    r.max_end_date as expected_renewal_date
from churn c
left join account_renewals r on c.account_id = r.account_id
