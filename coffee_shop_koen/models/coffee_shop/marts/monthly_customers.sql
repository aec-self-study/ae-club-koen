select
    customers.first_order_at as date,
    count(distinct customers.customer_id) as number_of_new_customers
from 
    {{ref('int_customers')}} customers
group by
    1