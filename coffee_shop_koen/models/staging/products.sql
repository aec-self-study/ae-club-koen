select 
    id as item_id,
    name,
    category,
    created_at
from 
    {{ source('coffee_shop', 'products') }}