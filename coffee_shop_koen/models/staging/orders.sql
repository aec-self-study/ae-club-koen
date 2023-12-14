
select 
    id as order_id,
    created_at,
    customer_id,
    total,
    address,
    state,
    zip,
from 
    {{ source('coffee_shop','orders') }}