{{ config(materialized='table') }}

with tickets as (
    select * from {{ ref('stg_support_tickets') }}
)

select
    ticket_id,
    account_id,
    created_date,
    resolved_date,
    severity,
    category,
    resolution_hours,
    cost_proxy,
    case when severity in ('P1', 'P2') then 1 else 0 end as is_high_severity
from tickets
