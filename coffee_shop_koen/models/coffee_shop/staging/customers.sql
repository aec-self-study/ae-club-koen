select 
  customers.id as customer_id, 
  customers.name,
  customers.email,
  min(orders.created_at) as first_order_at,
  count(orders.id) as number_of_orders,
  sum(orders.total) as total_spent
from 
  {{ source('coffee_shop', 'customers') }} customers
  join {{ source('coffee_shop', 'orders') }} orders
    on customers.id = orders.customer_id
group by
  1,2,3
order by
  first_order_at
limit 5
