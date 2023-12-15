with 
    first_visitor_id as (
        select distinct
            customer_id,
            first_value(visitor_id) over(partition by customer_id order by event_at rows unbounded preceding) as first_visitor_id
        from 
            {{ ref('stg_pageviews') }}
    ),
    with_last_event_at as (
        select 
            pageview_id,
            pageview_id||visitor_id as session_id,
            visitor_id,
            event_at,
            lag(event_at) over(partition by visitor_id order by event_at) as prev_event_at
        from 
            {{ ref('stg_pageviews') }}        
    ),

    time_since_last_event as (
        select 
            pageview_id,
            session_id,
            visitor_id,
            event_at,
            prev_event_at,
            date_diff(event_at, prev_event_at, minute) as minutes_since_last_event
        from 
            with_last_event_at      
    ),

    sessions as (
        select
            pageview_id,
            session_id,
            visitor_id,
            event_at,
            prev_event_at,
            minutes_since_last_event,
            sum(case when minutes_since_last_event > 30 or prev_event_at is null then 1 else null end) over (partition by visitor_id order by event_at) as session_nr
        from 
            time_since_last_event
    ),

    pageview_sessions as (
        select
            pageview_id,
            first_value(session_id) over(partition by visitor_id,session_nr order by event_at rows unbounded preceding) as session_id
        from 
            sessions
    )
select 
    pageviews.pageview_id,
    pageviews.visitor_id,
    pageviews.device_type,
    pageviews.event_at,
    pageviews.page,
    pageviews.customer_id,
    first_visitor_id.first_visitor_id,
    pageview_sessions.session_id
from 
    {{ ref('stg_pageviews') }} as pageviews
    left join first_visitor_id
        on pageviews.customer_id = first_visitor_id.customer_id
    left join pageview_sessions
        on pageviews.pageview_id = pageview_sessions.pageview_id