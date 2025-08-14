select
    app_id,
    date_day,
    device,
    source_type,
    source_relation,
    sum(crashes) as crashes
from {{ ref('stg_apple_store__app_crash_daily')}}
{{ dbt_utils.group_by(5) }}