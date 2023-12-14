select
    date_trunc(orders.created_at,'week') as sales_week,
    items.category as item_category,
    null as is_new_customer,
    sum(order_items.price) as total_income
from 
    {{ref('orders')}} orders 
    left join {{ref('int_order_items')}} order_items
        orders.order_id = order_items.order_id
    left join {{ref('products')}} as products
        on order_items.item_id = item.item_id
