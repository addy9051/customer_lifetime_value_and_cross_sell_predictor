{{ config(materialized='table') }}

with contracts_products as (
    select distinct product as product_name
    from {{ ref('stg_service_contracts') }}
    where product is not null
),

cross_sell_products as (
    select distinct top_1_product as product_name
    from {{ ref('stg_cross_sell_recommendations') }}
    where top_1_product is not null
    
    union
    
    select distinct top_2_product as product_name
    from {{ ref('stg_cross_sell_recommendations') }}
    where top_2_product is not null
),

distinct_products as (
    select product_name from contracts_products
    union
    select product_name from cross_sell_products
)

select
    -- Generate a surrogate ID via hashing the product name
    md5(product_name) as product_id,
    product_name,
    
    -- Assign business context mapping dynamically
    case
        when product_name = 'Neo' then 'Travel Booking Platform'
        when product_name = 'Egencia Analytics Studio' then 'Data & Insights'
        when product_name = 'Meetings & Events' then 'Event Management'
        when product_name = 'Travel Consulting' then 'Professional Services'
        else 'Other Service'
    end as product_category,
    
    -- Assign hypothetical margin/cost profiles for deeper BI analysis
    case
        when product_name = 'Neo' then 0.65
        when product_name = 'Egencia Analytics Studio' then 0.85
        when product_name = 'Meetings & Events' then 0.25
        when product_name = 'Travel Consulting' then 0.45
        else 0.50
    end as estimated_gross_margin_pct

from distinct_products
