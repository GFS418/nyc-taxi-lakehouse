-- The daily mart must account for every trip in the fact exactly once.
with mart as (select sum(trips) as trips from {{ ref('mart_daily_trip_metrics') }}),

fact as (select count(*) as trips from {{ ref('fct_trips') }})

select mart.trips as mart_trips, fact.trips as fact_trips
from mart cross join fact
where mart.trips != fact.trips
