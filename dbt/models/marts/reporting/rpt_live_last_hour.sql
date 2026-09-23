{{ config(materialized='view') }}

{#- Trips per minute over the last hour, as they arrive: the dashboard's live panel. -#}

select
    timestamp_trunc(publish_time, minute) as minute,
    countif(reject_reason is null) as trips,
    countif(reject_reason is not null) as rejected,
    sum(if(reject_reason is null, total_amount, 0)) as revenue
from {{ ref('stg_trip_events') }}
where publish_time >= timestamp_sub(current_timestamp(), interval 60 minute)
group by minute
