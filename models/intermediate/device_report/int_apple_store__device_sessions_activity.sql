select
    app_id,
    date_day,
    source_type,
    device,
    source_relation,
    sum(sessions) as sessions,
    sum(active_devices) as active_devices
from {{ ref('int_apple_store__session_daily') }}
{{ dbt_utils.group_by(5) }}