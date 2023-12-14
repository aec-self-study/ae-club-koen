
select 
    id as order_item_id,
    order_id,
    product_id
from 
    {{ source('coffee_shop','order_items') }}