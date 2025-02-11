with app_crashes as (
    select * 
    from {{ ref('int_apple_store__platform_version_app_crashes') }}
),

impressions_and_page_views as (
    select * 
    from {{ ref('int_apple_store__platform_version_impressions_pv') }}
),

downloads_daily as (
    select * 
    from {{ ref('int_apple_store__platform_version_downloads_daily') }}
),

install_deletions as (
    select * 
    from {{ ref('int_apple_store__platform_version_install_deletions') }}
),

sessions_activity as (
    select * 
    from {{ ref('int_apple_store__platform_version_sessions_activity') }}
),

-- Unifying all dimension values before aggregation
pre_reporting_grain as (
    select 
        app_id, 
        platform_version, 
        source_type, 
        source_relation 
    from app_crashes

    union all

    select 
        app_id, 
        platform_version, 
        source_type, 
        source_relation 
    from impressions_and_page_views

    union all

    select 
        app_id, 
        platform_version, 
        source_type, 
        source_relation 
    from downloads_daily

    union all

    select 
        app_id, 
        platform_version, 
        source_type, 
        source_relation 
    from install_deletions

    union all

    select 
        app_id, 
        platform_version, 
        source_type, 
        source_relation 
    from sessions_activity
)

-- Ensuring distinct combinations of all dimensions
select distinct
    app_id,
    platform_version,
    source_type,
    source_relation
from pre_reporting_grain