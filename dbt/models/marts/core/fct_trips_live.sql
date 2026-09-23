{{ config(materialized='view') }}

{#- Accepted live trips, shaped like fct_trips so the two can be served as one. -#}

select
    event_id as trip_id,
    -- Same convention as the batch fact: wall-clock time carried in a UTC-labeled TIMESTAMP.
    timestamp(pickup_datetime) as pickup_partition_ts,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    pickup_hour,
    trip_duration_seconds,
    vendor_id,
    ratecode_id,
    payment_type,
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance_miles,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    store_and_fwd_flag,
    is_zero_distance,
    is_zero_duration,
    is_long_duration,
    is_near_duplicate,
    coalesce(is_zero_distance or is_zero_duration or is_long_duration or is_near_duplicate, false)
        as has_quality_flag,
    publish_time
from {{ ref('stg_trip_events') }}
where reject_reason is null
