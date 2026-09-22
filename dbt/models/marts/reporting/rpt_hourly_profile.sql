{{ config(materialized='view') }}

{#- Hour of day by day of week: the shape a heatmap wants. -#}

select
    h.pickup_date,
    h.pickup_hour,
    d.year,
    d.day_name,
    d.day_of_week,
    d.is_weekend,
    h.trips,
    h.total_amount,
    h.avg_duration_seconds / 60 as avg_duration_minutes,
    h.avg_speed_mph
from {{ ref('mart_hourly_demand') }} as h
inner join {{ ref('dim_date') }} as d
    on h.pickup_date = d.date_day
