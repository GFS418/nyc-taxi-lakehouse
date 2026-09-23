{#-
  Typed, renamed view over curated trips.

  The source stores NYC local wall-clock time in UTC-labeled TIMESTAMPs, because BigQuery
  loads every Parquet timestamp as TIMESTAMP. DATETIME(ts, 'UTC') recovers the wall-clock
  value losslessly. pickup_partition_ts keeps the raw partition column: BigQuery prunes
  partitions only when a filter is on that column itself, not on an expression over it.
-#}

with source as (
    select * from {{ source('curated', 'yellow_trips') }}

    {#-
      CI builds a single month. Filtering here scopes every model and test downstream, so a pull
      request scans about a gibibyte instead of six years, and it prunes partitions on the source
      table's own partition column. Unset in dev and prod.
    -#}
    {% if var('min_month', none) %}
        where pickup_datetime >= timestamp(date '{{ var("min_month") }}-01')
    {% endif %}
)

select
    trip_id,
    vendor_id,
    pickup_datetime as pickup_partition_ts,
    datetime(pickup_datetime, 'UTC') as pickup_datetime,
    datetime(dropoff_datetime, 'UTC') as dropoff_datetime,
    date(pickup_datetime, 'UTC') as pickup_date,
    timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration_seconds,
    passenger_count,
    trip_distance as trip_distance_miles,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
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
    source_month,
    processed_at,
    is_zero_distance,
    is_zero_duration,
    is_long_duration,
    is_near_duplicate
from source
