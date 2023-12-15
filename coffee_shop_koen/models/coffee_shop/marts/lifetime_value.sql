{{
  config(
    materialized = "table",
    cluster_by = "week_start_date",
  )
}}
with 
    weeks as (
        select distinct
            week_start_date
        from 
            {{ref('date')}}
        where
            date_day >= '2021-01-01'
    ),

    customers as (
        select 
            customers.customer_id,
            cast(customers.first_order_at as date) as first_order_date
        from
            {{ref('customers')}} as customers
    ),

    sales as (
        select 
            cast(date_trunc(orders.created_at,week) as date) as order_week,
            orders.customer_id,
            orders.total
        from 
            {{ref('orders')}} as orders
    ),

    weekly_revenue as (
        select
            customers.customer_id,
            weeks.week_start_date,
            sum(sales.total) as weekly_revenue
        from 
            customers
            left join weeks 
                on customers.first_order_date <= weeks.week_start_date
            left join sales
                on customers.customer_id = sales.customer_id
                and weeks.week_start_date = sales.order_week
        group by
            1, 2
    )

select 
    weekly_revenue.customer_id,
    weekly_revenue.week_start_date,
    weekly_revenue.weekly_revenue,
    sum(weekly_revenue.weekly_revenue) over (partition by weekly_revenue.customer_id order by weekly_revenue.week_start_date rows unbounded preceding) as cumulative_revenue
from 
    weekly_revenue

