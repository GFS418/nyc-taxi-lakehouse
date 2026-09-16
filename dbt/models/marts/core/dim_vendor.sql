select
    vendor_id,
    vendor_name,
    in_current_dictionary
from {{ ref('vendors') }}
