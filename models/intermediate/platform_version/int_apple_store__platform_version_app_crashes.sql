select
    app_id,
    platform_version,
    date_day,
    source_type,
    source_relation,
    sum(crashes) as crashes
from {{ ref('stg_apple_store__app_crash_daily')}}
group by 1,2,3,4,5