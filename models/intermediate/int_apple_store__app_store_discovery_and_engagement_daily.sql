with base as (

    select *
    from {{ var('app_store_discovery_and_engagement_detailed_daily') }}
),

aggregated as (

    select 
        date_day,
        app_id,
        page_type,
        source_type,
        engagement_type,
        device,
        platform_version,
        territory,
        page_title,
        source_info,
        source_relation,
        sum(case when lower(event) = 'impression' then counts else 0 end) as impressions,
        sum(case when lower(event) = 'impression' then unique_counts else 0 end) as impressions_unique_device,
        sum(case when lower(event) = 'page view' then counts else 0 end) as page_views,
        sum(case when lower(event) = 'page view' then unique_counts else 0 end) as page_views_unique_device
    from base
    {{ dbt_utils.group_by(11) }}

)

select * 
from aggregated