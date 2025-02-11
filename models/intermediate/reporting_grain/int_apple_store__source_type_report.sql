with impressions_and_page_views as (
    select
        date_day,
        app_id,
        source_type,
        source_relation,
        sum(impressions) as impressions,
        sum(page_views) as page_views
    from {{ ref('int_apple_store__discovery_and_engagement_daily') }}
    group by 1,2,3,4
),

install_deletions as (
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
),

sessions_activity as (
    select
        date_day,
        app_id,
        source_type,
        source_relation,
        sum(active_devices) as active_devices,
        sum(sessions) as sessions
    from {{ ref('int_apple_store__session_daily') }}
    group by 1,2,3,4
),

-- Unifying all dimension values before aggregation
pre_reporting_grain as (
    select 
        app_id, 
        source_type, 
        source_relation 
    from impressions_and_page_views

    union all

    select 
        app_id, 
        source_type, 
        source_relation 
    from install_deletions

    union all

    select 
        app_id, 
        source_type, 
        source_relation 
    from sessions_activity
)

-- Ensuring distinct combinations of all dimensions
select distinct
    app_id,
    source_type,
    source_relation
from pre_reporting_grain
