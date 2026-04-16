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

, account_renewals as (
    select 
        account_id,
        max(end_date) as max_end_date
    from {{ ref('stg_service_contracts') }}
    group by 1
)

select 
    u.*,
    r.max_end_date as expected_renewal_date
from unpivoted u
left join account_renewals r on u.account_id = r.account_id
