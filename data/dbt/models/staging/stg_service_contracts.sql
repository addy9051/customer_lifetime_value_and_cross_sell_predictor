with source as (
    select * from {{ source('raw', 'service_contracts') }}
),

renamed as (
    select
        contract_id,
        account_id,
        product,
        start_date,
        end_date,
        contract_value,
        is_active,
        
        -- Derived logic
        datediff('month', start_date, end_date) as contract_months
        
    from source
)

select * from renamed
