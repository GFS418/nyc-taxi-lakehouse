select
    payment_type,
    payment_type_name,
    -- Tips are recorded automatically for card trips only; cash tips never appear.
    payment_type = 1 as tips_are_recorded
from {{ ref('payment_types') }}
