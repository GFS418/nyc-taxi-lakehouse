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
    {#-
      Two ways in.

      With a month var, rebuild exactly that month. The DAG passes its own data interval, so
      re-running any month, including an old one TLC republished, replaces just that partition.

      Without it, rebuild from the newest month present onward. _dbt_max_partition holds the
      largest VALUE of the partition column, not the start of its partition, so it has to be
      truncated: comparing against the raw timestamp keeps only the rows after the final trip
      of the month and silently drops the rest.
    -#}
    {% if var('month', none) %}
        where pickup_partition_ts >= timestamp(date '{{ var("month") }}-01')
            and pickup_partition_ts < timestamp(
                date_add(date '{{ var("month") }}-01', interval 1 month)
            )
    {% else %}
        where pickup_partition_ts >= timestamp_trunc(_dbt_max_partition, month)
    {% endif %}
{% endif %}
