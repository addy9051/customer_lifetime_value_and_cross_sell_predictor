with source as (
    select * from {{ source('raw', 'cross_sell_recommendations') }}
)

select
    account_id,
    top_1_product,
    top_1_score,
    top_2_product,
    top_2_score,
    num_products_current,
    current_date() as prediction_date
from source
