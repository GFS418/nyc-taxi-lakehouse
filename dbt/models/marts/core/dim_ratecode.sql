select
    ratecode_id,
    rate_code_name,
    ratecode_id = 99 as is_unknown_rate
from {{ ref('rate_codes') }}
