select
    date_day,
    app_id,
    source_type,
    source_relation,
    sum(impressions) as impressions,
    sum(page_views) as page_views
from {{ ref('int_apple_store__discovery_and_engagement_daily') }}
group by 1,2,3,4