with impressions_and_page_views as (
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
),

downloads_daily as (
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
),

install_deletions as (
    select
        app_id,
        date_day,
        source_type,
        territory,
        source_relation,
        sum(installations) as installations,
        sum(deletions) as deletions
    from {{ ref('int_apple_store__installation_and_deletion_daily') }}
    group by 1,2,3,4,5
),

sessions_activity as (
    select
        app_id,
        date_day,
        source_type,
        territory,
        source_relation,
        sum(sessions) as sessions,
        sum(active_devices) as active_devices
    from {{ ref('int_apple_store__session_daily') }}
    group by 1,2,3,4,5
),

country_codes as (
    
    select * 
    from {{ var('apple_store_country_codes') }}
),

-- Unifying all dimension values before aggregation
pre_reporting_grain as (
    select
        app_id, 
        source_type, 
        territory, 
        source_relation 
    from impressions_and_page_views

    union all

    select
        app_id, 
        source_type, 
        territory, 
        source_relation 
    from downloads_daily

    union all

    select
        app_id, 
        source_type, 
        territory, 
        source_relation 
    from install_deletions

    union all

    select
        app_id, 
        source_type, 
        territory, 
        source_relation 
    from sessions_activity
)

-- Ensuring distinct combinations of all dimensions
select distinct
    app_id,
    source_type,
    territory,
    source_relation
from pre_reporting_grain