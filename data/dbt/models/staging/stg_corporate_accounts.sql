with source as (
    select * from {{ source('raw', 'corporate_accounts') }}
),

renamed as (
    select
        account_id,
        company_name,
        industry,
        region,
        tier,
        onboarding_date,
        is_churned,
        churn_date,
        annual_contract_value,
        
        -- Derived features
        datediff('day', onboarding_date, coalesce(churn_date, current_date())) as tenure_days,
        
        case tier
            when 'Platinum' then 4
            when 'Gold'     then 3
            when 'Silver'   then 2
            else 1
        end as tier_rank
        
    from source
)

select * from renamed
