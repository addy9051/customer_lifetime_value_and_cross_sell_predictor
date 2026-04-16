with source as (
    select * from {{ source('raw', 'traveler_profiles') }}
)

select
    traveler_id,
    account_id,
    role,
    travel_tier
from source
