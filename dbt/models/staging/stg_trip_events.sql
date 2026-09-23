{{ config(materialized='view') }}

{#-
  Live trip events, deduplicated, then checked against the same row-level rules Spark applies to
  batch data, with the same reason codes in the same precedence.

  Two batch rules cannot apply to a single event. pickup_outside_source_month needs a source file,
  and reversal pairing needs the original charge, which may arrive later or never. Any negative
  amount is therefore rejected here as negative_amount_unmatched.
-#}

with deliveries as (
    select *
    from {{ source('stream', 'trip_events') }}
    -- Pub/Sub delivers at least once, and producers retry. The first delivery of each event wins.
    qualify row_number() over (partition by event_id order by publish_time, message_id) = 1
),

typed as (
    select
        event_id,
        event_ts,
        publish_time,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        date(pickup_datetime) as pickup_date,
        extract(hour from pickup_datetime) as pickup_hour,
        datetime_diff(dropoff_datetime, pickup_datetime, second) as trip_duration_seconds,
        passenger_count,
        trip_distance as trip_distance_miles,
        coalesce(ratecode_id, 99) as ratecode_id,
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
        coalesce(congestion_surcharge, 0) as congestion_surcharge,
        coalesce(airport_fee, 0) as airport_fee,
        coalesce(cbd_congestion_fee, 0) as cbd_congestion_fee,
        total_amount
    from deliveries
),

checked as (
    select
        *,
        case
            when vendor_id is null or pickup_datetime is null or dropoff_datetime is null
                or pu_location_id is null or do_location_id is null or payment_type is null
                or trip_distance_miles is null or fare_amount is null or total_amount is null
                then 'missing_required_field'
            when pu_location_id not between 1 and 265 or do_location_id not between 1 and 265
                then 'invalid_location_id'
            when dropoff_datetime < pickup_datetime then 'dropoff_before_pickup'
            when trip_duration_seconds > 24 * 3600 then 'duration_over_24h'
            when trip_distance_miles < 0 then 'negative_distance'
            when trip_distance_miles > 500 then 'distance_over_500_miles'
            when (
                trip_duration_seconds > 0
                and trip_distance_miles / (trip_duration_seconds / 3600) > 100
            ) or (trip_duration_seconds = 0 and trip_distance_miles > 0)
                then 'implied_speed_over_100_mph'
            when greatest(
                abs(fare_amount), abs(extra), abs(mta_tax), abs(tip_amount), abs(tolls_amount),
                abs(improvement_surcharge), abs(total_amount), abs(congestion_surcharge),
                abs(airport_fee), abs(cbd_congestion_fee)
            ) > 10000
                then 'amount_out_of_range'
            when least(
                fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
                total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee
            ) < 0
                then 'negative_amount_unmatched'
        end as reject_reason
    from typed
)

select
    *,
    trip_distance_miles = 0 as is_zero_distance,
    trip_duration_seconds = 0 as is_zero_duration,
    trip_duration_seconds > 3 * 3600 as is_long_duration,
    reject_reason is null and count(*) over (
        partition by
            vendor_id, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id,
            fare_amount, reject_reason is null
    ) > 1 as is_near_duplicate
from checked
