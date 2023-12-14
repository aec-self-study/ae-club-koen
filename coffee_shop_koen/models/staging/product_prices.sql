select 
    id as product_price_id,
    product_id,
    price,
    created_at,
    ended_at
from 
    {{source('coffee_shop','product_prices')}} 