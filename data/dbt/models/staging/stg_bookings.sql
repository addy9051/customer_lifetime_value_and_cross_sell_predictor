with source as (
    select * from {{ source('raw', 'bookings') }}
),

travelers as (
    select traveler_id, account_id 
    from {{ source('raw', 'traveler_profiles') }}
),

renamed as (
    select
        b.booking_id,
        b.traveler_id,
        t.account_id,
        b.booking_type,
        b.booking_date,
        b.travel_date,
        b.amount,
        b.is_out_of_policy,
        b.is_cancelled,
        b.destination_region,
        
        -- Derived features
        datediff('day', b.booking_date, b.travel_date) as lead_time_days
        
    from source b
    left join travelers t on b.traveler_id = t.traveler_id
)

select * from renamed
