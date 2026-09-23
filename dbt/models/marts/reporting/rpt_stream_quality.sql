{{ config(materialized='view') }}

{#- What became of every delivery in the last 24 hours: kept, rejected by a rule, or a duplicate. -#}

with deliveries as (
    select count(*) as n
    from {{ source('stream', 'trip_events') }}
    where publish_time >= timestamp_sub(current_timestamp(), interval 24 hour)
),

events as (
    select coalesce(reject_reason, 'kept') as outcome, count(*) as n
    from {{ ref('stg_trip_events') }}
    where publish_time >= timestamp_sub(current_timestamp(), interval 24 hour)
    group by outcome
)

select outcome, n as events from events

union all

select
    'duplicate_delivery' as outcome,
    deliveries.n - coalesce((select sum(n) from events), 0) as events
from deliveries
