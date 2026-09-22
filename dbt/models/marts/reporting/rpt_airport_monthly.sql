{{ config(materialized='view') }}

{#- Airport traffic by month, with the airport's own name attached. -#}

select
    a.month_start,
    extract(year from a.month_start) as year,
    a.airport_code,
    case a.airport_code
        when 'JFK' then 'JFK Airport'
        when 'LGA' then 'LaGuardia Airport'
        when 'EWR' then 'Newark Airport'
    end as airport_name,
    a.direction,
    a.trips,
    a.avg_total_amount,
    a.avg_distance_miles,
    a.avg_duration_seconds / 60 as avg_duration_minutes
from {{ ref('mart_airport_trips') }} as a
