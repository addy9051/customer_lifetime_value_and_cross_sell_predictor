{{ config(materialized='table') }}

with tickets as (
    select * from {{ ref('stg_support_tickets') }}
)

select
    ticket_id,
    account_id,
    created_date,
    coalesce(resolved_date, '9999-12-31') as resolved_date,
    severity,
    category,
    coalesce(resolution_hours, 0) as resolution_hours,
    cost_proxy,
    case when severity in ('P1', 'P2') then 1 else 0 end as is_high_severity
from tickets
