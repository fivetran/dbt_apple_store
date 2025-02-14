select
    app_id,
    date_day,
    source_type,
    device,
    source_relation,
    sum(impressions) as impressions,
    sum(impressions_unique_device) as impressions_unique_device,
    sum(page_views) as page_views,
    sum(page_views_unique_device) as page_views_unique_device
from {{ ref('int_apple_store__discovery_and_engagement_daily') }}
{{ dbt_utils.group_by(5) }}