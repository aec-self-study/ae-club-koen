select
    customers.first_order_at,
    count(distinct customers.id) as number_of_new_customers
from 
    {{ref('customers')}} customers
group by
    1