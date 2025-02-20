select
    app_id,
    app_version,
    date_day,
    source_type,
    source_relation,
    sum(installations) as installations,
    sum(deletions) as deletions
from {{ ref('int_apple_store__installation_and_deletion_daily') }}
group by 1,2,3,4,5