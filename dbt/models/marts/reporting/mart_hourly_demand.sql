{#- Demand by local hour: the shape of a taxi day, and the input to any staffing question. -#}

select
    pickup_date,
    pickup_hour,
    count(*) as trips,
    sum(total_amount) as total_amount,
    avg(trip_duration_seconds) as avg_duration_seconds,
    -- Average speed over the hour, not the average of per-trip speeds.
    safe_divide(sum(trip_distance_miles), safe_divide(sum(trip_duration_seconds), 3600)) as avg_speed_mph
from {{ ref('fct_trips') }}
group by pickup_date, pickup_hour
