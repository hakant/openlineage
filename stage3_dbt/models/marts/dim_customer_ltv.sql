select
    c.customer_id,
    c.customer_name,
    c.email,
    c.country,
    count(o.order_id)                 as lifetime_orders,
    coalesce(sum(o.amount), 0)        as lifetime_value
from {{ ref('stg_customers') }} as c
left join {{ ref('stg_orders') }} as o
    on o.customer_id = c.customer_id
   and o.status = 'completed'
group by c.customer_id, c.customer_name, c.email, c.country
