{{ config(materialized='table') }}

with bookings as (
    select * from {{ ref('stg_bookings') }}
)

select
    booking_id,
    account_id,
    booking_date,
    travel_date,
    coalesce(amount, 0) as transaction_amount,
    is_out_of_policy,
    coalesce(lead_time_days, 0) as lead_time_days
from bookings
