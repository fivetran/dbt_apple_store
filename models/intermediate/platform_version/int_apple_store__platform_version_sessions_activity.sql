select
    app_id,
    platform_version,
    date_day,
    source_type,
    source_relation,
    sum(sessions) as sessions,
    sum(active_devices) as active_devices
from {{ ref('int_apple_store__session_daily') }}
group by 1,2,3,4,5