select
    order_id,
    customer_id,
    cast(order_ts as timestamp)       as order_ts,
    cast(order_ts as date)            as order_date,
    amount_cents / 100.0              as amount,
    status
from {{ source('shop_raw', 'raw_orders') }}
where status <> 'cancelled'
