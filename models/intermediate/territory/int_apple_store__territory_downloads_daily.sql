select
    app_id,
    date_day,
    source_type,
    territory,
    source_relation,
    sum(first_time_downloads) as first_time_downloads,
    sum(redownloads) as redownloads,
    sum(total_downloads) as total_downloads
from {{ ref('int_apple_store__download_daily') }}
group by 1,2,3,4,5