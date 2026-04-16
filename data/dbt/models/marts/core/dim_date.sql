{{ config(materialized='table') }}

with date_spine as (
    select
        cast(dateadd(day, seq4(), to_date('2020-01-01')) as date) as date_day
    from table(generator(rowcount => 3653))  -- full 2020-01-01 to 2029-12-31
)

select
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    monthname(date_day) as month_name,
    quarter(date_day) as quarter,
    dayofweek(date_day) as day_of_week,
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend,
    dayofmonth(date_day) as day_of_month,
    date_trunc('month', date_day) as first_day_of_month,
    date_trunc('quarter', date_day) as first_day_of_quarter,
    date_trunc('year', date_day) as first_day_of_year
from date_spine