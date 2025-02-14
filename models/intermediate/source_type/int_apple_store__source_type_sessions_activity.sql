select
    date_day,
    app_id,
    source_type,
    source_relation,
    sum(active_devices) as active_devices,
    sum(sessions) as sessions
from {{ ref('int_apple_store__session_daily') }}
group by 1,2,3,4