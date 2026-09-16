-- The recorded-passenger denominator can never exceed total trips.
select pickup_date, trips, trips_with_recorded_passengers
from {{ ref('mart_daily_trip_metrics') }}
where trips_with_recorded_passengers > trips
