{{ config(materialized='table') }}

with date_spine as (
  select
    date_add('2022-01-01', interval row_number() over () - 1 day) as calendar_date
  from unnest(generate_array(1, 4000)) 
)

select
  calendar_date,
  extract(year from calendar_date) as year,
  extract(month from calendar_date) as month,
  format_date('%Y-%m', calendar_date) as year_month,
  extract(week from calendar_date) as week,
  format_date('%Y-%W', calendar_date) as year_week,
  extract(dayofweek from calendar_date) as day_of_week,
  case
    when extract(dayofweek from calendar_date) in (1, 7) then true
    else false
  end as is_weekend
from date_spine