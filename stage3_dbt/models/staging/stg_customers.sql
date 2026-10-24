select
    customer_id,
    name                              as customer_name,
    lower(email)                      as email,
    country
from {{ source('shop_raw', 'raw_customers') }}
