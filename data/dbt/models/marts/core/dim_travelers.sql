{{ config(materialized='table') }}

with travelers as (
    select * from {{ ref('stg_traveler_profiles') }}
)

select
    traveler_id,
    account_id,
    coalesce(nullif(role, ''), 'Unspecified Role') as role,
    coalesce(nullif(travel_tier, ''), 'Standard') as travel_tier
from travelers
