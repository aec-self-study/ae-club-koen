select
    id as pageview_id,
    visitor_id,
    device_type,
    `timestamp` as event_at,
    page,
    customer_id,
from 
    {{ source('web_tracking','pageviews') }}