{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'month_start', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['pu_location_id', 'do_location_id'],
    )
}}

{#- Monthly zone-to-zone flows. Incremental for the same reason the fact is: one month per run. -#}

select
    date_trunc(pickup_date, month) as month_start,
    pu_location_id,
    do_location_id,
    count(*) as trips,
    sum(total_amount) as total_amount,
    avg(trip_distance_miles) as avg_distance_miles,
    avg(trip_duration_seconds) as avg_duration_seconds
from {{ ref('fct_trips') }}

{% if is_incremental() %}
    {% if var('month', none) %}
        where pickup_date >= date '{{ var("month") }}-01'
            and pickup_date < date_add(date '{{ var("month") }}-01', interval 1 month)
    {% else %}
        where date_trunc(pickup_date, month) >= (select max(month_start) from {{ this }})
    {% endif %}
{% endif %}

group by month_start, pu_location_id, do_location_id
