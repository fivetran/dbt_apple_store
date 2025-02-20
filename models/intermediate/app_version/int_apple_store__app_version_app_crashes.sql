select
    app_id,
    app_version,
    date_day,
    source_type,
    source_relation,
    sum(crashes) as crashes
from {{ var('app_crash_daily') }}
group by 1,2,3,4,5