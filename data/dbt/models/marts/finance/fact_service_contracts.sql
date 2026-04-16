{{ config(materialized='table') }}

with contracts as (
    select * from {{ ref('stg_service_contracts') }}
)

select
    contract_id,
    account_id,
    start_date,
    end_date,
    product as service_product,
    contract_months,
    contract_value,
    is_active
from contracts
