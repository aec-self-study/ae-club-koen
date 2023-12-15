with 
    sessions_stats as (
        select
            session_id,
            count(pageview_id) as pages_visited,
            min(event_at) as session_start_time,
            max(event_at) as session_end_time,
            case when max(customer_id) is not null then true else false end as has_purchase
        from 
            {{ ref('int_pageviews') }}
        group by
            1
    )

select
    *
from
    sessions_stats