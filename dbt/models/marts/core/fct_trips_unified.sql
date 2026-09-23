{{ config(materialized='view') }}

{#-
  The serving layer of a lambda architecture.

  History comes from the batch fact: complete and fully validated, but about two months behind,
  because that is when TLC publishes. The tail after the batch high-water mark comes from the
  stream, validated event by event. When the monthly DAG loads a new month the high-water mark
  moves, and those trips switch from stream to batch on the next build, so none is served twice.

  Filter on pickup_partition_ts to prune the batch side's partitions.
-#}

{% set columns %}
    trip_id,
    pickup_partition_ts,
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
    tip_amount,
    total_amount,
    has_quality_flag
{% endset %}

select {{ columns }}, 'batch' as served_from
from {{ ref('fct_trips') }}

union all

select {{ columns }}, 'stream' as served_from
from {{ ref('fct_trips_live') }}
where pickup_datetime > (select batch_high_water from {{ ref('batch_high_water_mark') }})
