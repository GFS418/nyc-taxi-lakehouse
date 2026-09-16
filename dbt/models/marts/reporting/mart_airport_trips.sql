{#-
  Monthly airport traffic. A trip counts once: if it starts at an airport it is a pickup, even
  when it also ends at one, so airport-to-airport trips are never double counted.
-#}

with trips as (
    select
        f.pickup_date,
        f.total_amount,
        f.trip_duration_seconds,
        f.trip_distance_miles,
        pickup_zone.airport_code as pickup_airport,
        dropoff_zone.airport_code as dropoff_airport
    from {{ ref('fct_trips') }} as f
    inner join {{ ref('dim_zone') }} as pickup_zone
        on f.pu_location_id = pickup_zone.location_id
    inner join {{ ref('dim_zone') }} as dropoff_zone
        on f.do_location_id = dropoff_zone.location_id
    where pickup_zone.is_airport or dropoff_zone.is_airport
)

select
    date_trunc(pickup_date, month) as month_start,
    coalesce(pickup_airport, dropoff_airport) as airport_code,
    if(pickup_airport is not null, 'pickup', 'dropoff') as direction,
    count(*) as trips,
    avg(total_amount) as avg_total_amount,
    avg(trip_distance_miles) as avg_distance_miles,
    avg(trip_duration_seconds) as avg_duration_seconds
from trips
group by month_start, airport_code, direction
