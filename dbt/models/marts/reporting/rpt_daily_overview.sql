{{ config(materialized='view') }}

{#-
  Daily metrics with calendar attributes already joined on. BI tools should not have to know how
  this schema fits together, and a view costs no storage and is never stale.
-#}

select
    m.pickup_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.month_start,
    d.day_name,
    d.is_weekend,
    m.trips,
    m.flagged_trips,
    m.trip_distance_miles,
    m.fare_amount,
    m.tip_amount,
    m.total_amount,
    m.card_tip_rate,
    m.avg_passengers_per_recorded_trip,
    safe_divide(m.total_amount, m.trips) as avg_fare_per_trip,
    safe_divide(m.trip_distance_miles, m.trips) as avg_miles_per_trip
from {{ ref('mart_daily_trip_metrics') }} as m
inner join {{ ref('dim_date') }} as d
    on m.pickup_date = d.date_day
