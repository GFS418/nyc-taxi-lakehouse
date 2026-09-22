{{ config(materialized='view') }}

{#-
  Borough-to-borough flows by month. Small enough (a few thousand rows) that a public dashboard can
  hold a cached extract of it, instead of querying BigQuery for every visitor.
-#}

select
    month_start,
    pickup_borough,
    dropoff_borough,
    sum(trips) as trips,
    sum(total_amount) as total_amount,
    safe_divide(sum(total_amount), sum(trips)) as avg_fare_per_trip
from {{ ref('rpt_zone_flows_named') }}
where not has_unknown_zone
group by month_start, pickup_borough, dropoff_borough
