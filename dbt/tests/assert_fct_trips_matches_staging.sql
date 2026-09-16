-- The fact must carry every staged trip exactly once: no drops, no duplication from a re-run.
with fact as (select count(*) as trips from {{ ref('fct_trips') }}),

staging as (select count(*) as trips from {{ ref('stg_yellow_trips') }})

select fact.trips as fact_trips, staging.trips as staging_trips
from fact cross join staging
where fact.trips != staging.trips
