{#- One row per calendar day, generated rather than derived from the facts. -#}

with days as (
    select date_day
    from unnest(generate_date_array(
        '{{ var("date_start", "2019-01-01") }}',
        '{{ var("date_end", "2025-12-31") }}'
    )) as date_day
)

select
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    format_date('%B', date_day) as month_name,
    extract(day from date_day) as day_of_month,
    extract(dayofweek from date_day) as day_of_week,
    format_date('%A', date_day) as day_name,
    extract(dayofweek from date_day) in (1, 7) as is_weekend,
    extract(isoweek from date_day) as iso_week,
    date_trunc(date_day, month) as month_start
from days
