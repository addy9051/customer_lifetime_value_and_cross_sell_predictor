{{ config(materialized='table') }}

with bookings as (
    select * from {{ ref('stg_bookings') }}
)

select
    booking_id,
    account_id,
    booking_date,
    travel_date,
    amount as transaction_amount,
    is_out_of_policy,
    lead_time_days
from bookings
