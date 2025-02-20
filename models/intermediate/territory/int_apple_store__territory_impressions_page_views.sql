select
    app_id,
    date_day,
    source_type,
    territory,
    source_relation,
    sum(impressions) as impressions,
    sum(impressions_unique_device) as impressions_unique_device,
    sum(page_views) as page_views,
    sum(page_views_unique_device) as page_views_unique_device
from {{ ref('int_apple_store__discovery_and_engagement_daily') }}
group by 1,2,3,4,5