{{ config(materialized='table') }}

with cross_sell as (
    select * from {{ ref('stg_cross_sell_recommendations') }}
)

, unpivoted as (
    select
        account_id,
        prediction_date,
        1 as recommendation_rank,
        top_1_product as recommended_product,
        top_1_score as probability_score,
        num_products_current
    from cross_sell
    where top_1_product is not null

    union all

    select
        account_id,
        prediction_date,
        2 as recommendation_rank,
        top_2_product as recommended_product,
        top_2_score as probability_score,
        num_products_current
    from cross_sell
    where top_2_product is not null
)

, account_dims as (
    select account_id, industry, tier, base_acv from {{ ref('dim_accounts') }}
)

, calibrated_recommendations as (
    select
        u.account_id,
        u.prediction_date,
        u.recommendation_rank,
        u.recommended_product,
        -- Apply Bias: Tech + Analytics Studio (+0.25), FinServ + Meetings (+0.20)
        (u.probability_score + 
            case 
                when d.industry = 'Technology' and u.recommended_product = 'Egencia Analytics Studio' then 0.25
                when d.industry = 'Financial Services' and u.recommended_product = 'Meetings & Events' then 0.20
                when d.industry = 'Energy' and u.recommended_product = 'Neo' then 0.15
                else 0 
            end +
            case
                when d.tier = 'Platinum' then 0.15
                when d.tier = 'Gold' then 0.05
                else 0
            end
        ) as adjusted_score,
        u.num_products_current,
        d.industry,
        d.tier,
        d.base_acv
    from unpivoted u
    left join account_dims d on u.account_id = d.account_id
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
    c.recommendation_rank,
    c.recommended_product,
    least(1.0, greatest(0.0, c.adjusted_score)) as probability_score,
    c.num_products_current,
    c.industry,
    c.tier,
    c.base_acv,
    r.max_end_date as expected_renewal_date
from calibrated_recommendations c
left join account_renewals r on c.account_id = r.account_id
