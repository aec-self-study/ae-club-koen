select 
    id as product_id,
    name,
    category,
    created_at
from 
    {{ source('coffee_shop', 'products') }}