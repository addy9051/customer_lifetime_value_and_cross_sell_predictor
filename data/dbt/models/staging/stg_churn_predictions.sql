with source as (
    select * from {{ source('raw', 'churn_predictions') }}
)

select
    account_id,
    churn_risk_score,
    survival_prob_30d,
    survival_prob_90d,
    survival_prob_180d,
    survival_prob_365d,
    expected_lifetime_days,
    current_date() as prediction_date
from source
