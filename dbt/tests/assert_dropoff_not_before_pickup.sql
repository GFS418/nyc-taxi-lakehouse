-- Spark quarantines these rows; this test proves none leaked into the warehouse.
select trip_id, pickup_datetime, dropoff_datetime
from {{ ref('stg_yellow_trips') }}
where dropoff_datetime < pickup_datetime
