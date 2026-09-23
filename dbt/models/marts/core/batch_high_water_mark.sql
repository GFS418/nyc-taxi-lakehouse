{{ config(materialized='table') }}

{#-
  Where batch coverage ends. Materialized once per dbt run so the unified view never rescans the
  fact: finding the maximum costs about 2 GiB, paid at build time instead of on every query.
-#}

select max(pickup_datetime) as batch_high_water
from {{ ref('fct_trips') }}
