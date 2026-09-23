{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'pickup_partition_ts', 'data_type': 'timestamp', 'granularity': 'month'},
        cluster_by=['pu_location_id', 'do_location_id'],
        on_schema_change='fail',
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
    source_month,
    is_zero_distance,
    is_zero_duration,
    is_long_duration,
    is_near_duplicate,
    coalesce(is_zero_distance or is_zero_duration or is_long_duration or is_near_duplicate, false)
        as has_quality_flag
from {{ ref('stg_yellow_trips') }}

{#-
  Three windows can apply, and they combine:

  - `month`: rebuild exactly that month. The DAG passes its own data interval, so re-running any
    month, including an old one TLC republished, replaces just that partition.
  - incremental with no month: rebuild from the newest month present onward. _dbt_max_partition
    holds the largest VALUE of the partition column, not the start of its partition, so it must be
    truncated. Comparing against the raw timestamp keeps only the rows after the month's final trip
    and silently drops the rest.
  CI narrows the build through `min_month` on the staging view instead, which scopes the tests too.
-#}
{%- set filters = [] %}
{%- if var('month', none) %}
    {%- do filters.append("pickup_partition_ts >= timestamp(date '" ~ var('month') ~ "-01')") %}
    {%- do filters.append(
        "pickup_partition_ts < timestamp(date_add(date '" ~ var('month') ~ "-01', interval 1 month))"
    ) %}
{%- elif is_incremental() %}
    {%- do filters.append("pickup_partition_ts >= timestamp_trunc(_dbt_max_partition, month)") %}
{%- endif %}

{% if filters %}where {{ filters | join("\n    and ") }}{% endif %}
