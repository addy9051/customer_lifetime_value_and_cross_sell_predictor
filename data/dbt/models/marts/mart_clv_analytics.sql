with accounts as (
    select * from {{ ref('stg_corporate_accounts') }}
),

bookings as (
    select
        account_id,
        count(booking_id) as total_bookings,
        sum(amount) as total_booking_amount,
        avg(amount) as avg_booking_amount,
        sum(case when is_out_of_policy then 1 else 0 end) as out_of_policy_bookings,
        avg(lead_time_days) as avg_lead_time_days
    from {{ ref('stg_bookings') }}
    group by account_id
),

tickets as (
    select
        account_id,
        count(ticket_id) as total_support_tickets,
        sum(cost_proxy) as total_support_cost_proxy,
        sum(case when severity in ('P1', 'P2') then 1 else 0 end) as high_severity_tickets
    from {{ ref('stg_support_tickets') }}
    group by account_id
),

contracts as (
    select
        account_id,
        sum(contract_value) as total_contract_value,
        count(contract_id) as active_contracts,
        avg(contract_months) as avg_contract_duration
    from {{ ref('stg_service_contracts') }}
    where is_active = true
    group by account_id
)

select
    a.account_id,
    a.company_name,
    a.industry,
    a.region,
    a.tier,
    a.onboarding_date,
    a.is_churned,
    a.churn_date,
    a.annual_contract_value as base_acv,
    a.tenure_days,
    
    -- Booking KPIs
    coalesce(b.total_bookings, 0) as total_bookings,
    coalesce(b.total_booking_amount, 0) as total_booking_amount,
    coalesce(b.avg_booking_amount, 0) as avg_booking_amount,
    coalesce(b.out_of_policy_bookings, 0) as out_of_policy_bookings,
    
    -- Support KPIs
    coalesce(t.total_support_tickets, 0) as total_support_tickets,
    coalesce(t.high_severity_tickets, 0) as high_severity_tickets,
    coalesce(t.total_support_cost_proxy, 0) as total_support_cost_proxy,
    
    -- Contract KPIs
    coalesce(c.total_contract_value, 0) as active_contract_value,
    coalesce(c.active_contracts, 0) as active_contracts,
    
    -- BI Derived Metrics
    case when a.tenure_days > 0 
         then (coalesce(b.total_booking_amount, 0) / a.tenure_days) * 365 
         else 0 
    end as annualized_booking_run_rate,
    
    case when coalesce(b.total_bookings, 0) > 0 
         then (coalesce(b.out_of_policy_bookings, 0)::float / b.total_bookings)
         else 0 
    end as out_of_policy_rate
    
from accounts a
left join bookings b on a.account_id = b.account_id
left join tickets t on a.account_id = t.account_id
left join contracts c on a.account_id = c.account_id
