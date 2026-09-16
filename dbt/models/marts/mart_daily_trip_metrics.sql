{#-
  One row per pickup date (NYC local). Tip rate uses card trips only, because the TLC
  records tips automatically for card payments and never records cash tips. Passenger
  averages divide by trips with a recorded count: SUM skips NULLs, but COUNT(*) does not.
-#}

select
    pickup_date,
    count(*) as trips,
    countif(passenger_count is not null) as trips_with_recorded_passengers,
    sum(passenger_count) as recorded_passengers,
    safe_divide(sum(passenger_count), countif(passenger_count is not null))
        as avg_passengers_per_recorded_trip,
    sum(trip_distance_miles) as trip_distance_miles,
    sum(fare_amount) as fare_amount,
    sum(tip_amount) as tip_amount,
    sum(total_amount) as total_amount,
    safe_divide(
        sum(if(payment_type = 1, tip_amount, 0)),
        sum(if(payment_type = 1, fare_amount, 0))
    ) as card_tip_rate
from {{ ref('stg_yellow_trips') }}
group by pickup_date
