{{ config(materialized='table') }}

with travelers as (
    select * from {{ ref('stg_traveler_profiles') }}
)

select
    traveler_id,
    account_id,
    role,
    travel_tier
from travelers
