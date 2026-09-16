-- The mart must account for every staged trip exactly once.
with mart as (
    select sum(trips) as trips from {{ ref('mart_daily_trip_metrics') }}
),

staging as (
    select count(*) as trips from {{ ref('stg_yellow_trips') }}
)

select mart.trips as mart_trips, staging.trips as staging_trips
from mart cross join staging
where mart.trips != staging.trips
