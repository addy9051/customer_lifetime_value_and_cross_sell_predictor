with source as (
    select * from {{ source('raw', 'support_tickets') }}
),

renamed as (
    select
        ticket_id,
        account_id,
        created_date,
        resolved_date,
        severity,
        category,
        resolution_hours,
        
        -- Derived features
        case severity
            when 'P1' then 2000
            when 'P2' then 500
            when 'P3' then 150
            when 'P4' then 50
            else 0
        end as cost_proxy
        
    from source
)

select * from renamed
