{#- Taxi zones, playing two roles in the fact: pickup and dropoff. -#}

select
    location_id,
    borough,
    zone_name,
    service_zone,
    location_id in (1, 132, 138) as is_airport,
    case location_id
        when 1 then 'EWR'
        when 132 then 'JFK'
        when 138 then 'LGA'
    end as airport_code,
    -- 264 and 265 are the TLC's own placeholders, not data errors.
    location_id in (264, 265) as is_unknown_zone
from {{ ref('taxi_zones') }}
