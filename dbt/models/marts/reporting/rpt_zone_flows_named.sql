{{ config(materialized='view') }}

{#- Zone-to-zone flows with borough and zone names resolved, so a chart can label itself. -#}

select
    f.month_start,
    f.pu_location_id,
    f.do_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone_name as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone_name as dropoff_zone,
    pickup_zone.is_airport as pickup_is_airport,
    dropoff_zone.is_airport as dropoff_is_airport,
    pickup_zone.is_unknown_zone or dropoff_zone.is_unknown_zone as has_unknown_zone,
    f.trips,
    f.total_amount,
    f.avg_distance_miles,
    f.avg_duration_seconds / 60 as avg_duration_minutes
from {{ ref('mart_zone_flows') }} as f
inner join {{ ref('dim_zone') }} as pickup_zone
    on f.pu_location_id = pickup_zone.location_id
inner join {{ ref('dim_zone') }} as dropoff_zone
    on f.do_location_id = dropoff_zone.location_id
