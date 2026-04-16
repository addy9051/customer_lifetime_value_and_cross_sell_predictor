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

, support_summary as (
    select
        account_id,
        count(*) as support_ticket_count
    from {{ ref('stg_support_tickets') }}
    group by 1
)

, account_dims as (
    select account_id, industry, tier from {{ ref('dim_accounts') }}
)

, calibrated_churn as (
    select
        c.account_id,
        c.prediction_date,
        -- Apply Bias: Retail (+0.3), Technology (-0.15), Platinum (-0.1)
        (coalesce(c.churn_risk_score, 0) + 
            case 
                when d.industry = 'Retail' then 0.35
                when d.industry = 'Energy' then 0.25
                when d.industry = 'Technology' then -0.15
                when d.industry = 'Financial Services' then -0.20
                else 0 
            end +
            case
                when d.tier = 'Platinum' then -0.15
                when d.tier = 'Bronze' then 0.15
                else 0
            end
        ) as adjusted_score,
        c.survival_prob_30d,
        c.survival_prob_90d,
        c.survival_prob_180d,
        c.survival_prob_365d,
        c.expected_lifetime_days
    from churn c
    left join account_dims d on c.account_id = d.account_id
)

select
    c.account_id,
    c.prediction_date,
    least(1.0, greatest(0.0, c.adjusted_score)) as churn_risk_score,
    case
        when c.adjusted_score > 0.5 then 'High'
        when c.adjusted_score > 0.25 then 'Medium'
        else 'Low'
    end as risk_level,
    coalesce(c.survival_prob_30d, 1) as survival_prob_30d,
    coalesce(c.survival_prob_90d, 1) as survival_prob_90d,
    coalesce(c.survival_prob_180d, 1) as survival_prob_180d,
    coalesce(c.survival_prob_365d, 1) as survival_prob_365d,
    coalesce(c.expected_lifetime_days, 3650) as expected_lifetime_days,
    r.max_end_date as expected_renewal_date,
    coalesce(s.support_ticket_count, 0) as support_ticket_count,
    d.industry,
    d.tier
from calibrated_churn c
left join account_renewals r on c.account_id = r.account_id
left join support_summary s on c.account_id = s.account_id
left join account_dims d on c.account_id = d.account_id
