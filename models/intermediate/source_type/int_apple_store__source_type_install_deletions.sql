select
    date_day,
    app_id,
    source_type,
    source_relation,
    sum(first_time_downloads) as first_time_downloads,
    sum(redownloads) as redownloads,
    sum(total_downloads) as total_downloads,
    sum(deletions) as deletions,
    sum(installations) as installations
from {{ ref('int_apple_store__installation_and_deletion_daily') }}
group by 1,2,3,4