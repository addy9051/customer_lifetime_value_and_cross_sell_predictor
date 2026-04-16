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
    coalesce(contract_months, 0) as contract_months,
    coalesce(contract_value, 0) as contract_value,
    is_active
from contracts
