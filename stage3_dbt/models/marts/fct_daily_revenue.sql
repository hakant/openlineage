select
    order_date,
    sum(amount)                       as revenue,
    count(order_id)                   as orders,
    avg(amount)                       as avg_order_value
from {{ ref('stg_orders') }}
where status = 'completed'
group by order_date
