{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'pickup_partition_ts', 'data_type': 'timestamp', 'granularity': 'month'},
        cluster_by=['pu_location_id', 'do_location_id'],
    )
}}

{#-
  One row per completed trip: the grain the whole model is built on.

  Incremental by month. A run rebuilds only the months present in the new data and swaps those
  partitions in whole, so a routine build scans one month instead of 219 million rows. A full
  rebuild is always available with --full-refresh.
-#}

select
    trip_id,
    pickup_partition_ts,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    extract(hour from pickup_datetime) as pickup_hour,
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
    source_month
from {{ ref('stg_yellow_trips') }}

{% if is_incremental() %}
    -- _dbt_max_partition is the newest month already in the table. Months from there on are
    -- rebuilt and replaced whole, which is what makes a re-run safe.
    where pickup_partition_ts >= _dbt_max_partition
{% endif %}
