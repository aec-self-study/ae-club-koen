select
    order_items.order_item_id,
    order_items.order_id,
    order_items.product_id,
    product_prices.price as item_price,

from 
    {{ref('order_items')}} as order_items
    join {{ref('orders')}} as orders
        on order_items.order_id = orders.order_id
    join {{ref('product_prices')}} as product_prices
        on orders.created_at between product_prices.created_at and product_prices.ended_at
        and order_items.product_id = product_prices.product_id
