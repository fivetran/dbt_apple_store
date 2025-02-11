with date_spine as (
    select
        date_day 
    from {{ ref('int_apple_store__date_spine') }}
),

app as (
    select
        app_id,
        app_name,
        source_relation
    from {{ var('app_store_app') }}
),

app_crashes as (
    select
        app_id,
        platform_version,
        date_day,
        source_type,
        source_relation,
        sum(crashes) as crashes
    from {{ var('app_crash_daily') }}
    group by 1,2,3,4,5
),

impressions_and_page_views as (
    select
        app_id,
        platform_version,
        date_day,
        source_type,
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
        platform_version,
        date_day,
        source_type,
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
        platform_version,
        date_day,
        source_type,
        source_relation,
        sum(installations) as installations,
        sum(deletions) as deletions
    from {{ ref('int_apple_store__installation_and_deletion_daily') }}
    group by 1,2,3,4,5
),

sessions_activity as (
    select
        app_id,
        platform_version,
        date_day,
        source_type,
        source_relation,
        sum(sessions) as sessions,
        sum(active_devices) as active_devices
    from {{ ref('int_apple_store__session_daily') }}
    group by 1,2,3,4,5
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
),

-- Ensuring distinct combinations of all dimensions
distinct_combos as (
    select distinct
        app_id,
        platform_version,
        source_type,
        source_relation
    from pre_reporting_grain
),

reporting_grain as (
    select
        ds.date_day,
        ug.app_id,
        ug.platform_version,
        ug.source_type, 
        ug.source_relation
    from date_spine as ds
    cross join distinct_combos as ug
),

-- Final aggregation using reporting grain
final as (
    select
        rg.source_relation,
        rg.date_day,
        rg.app_id,
        a.app_name,
        rg.source_type,
        rg.platform_version,
        coalesce(ac.crashes, 0) as crashes,
        coalesce(ip.impressions, 0) as impressions,
        coalesce(ip.impressions_unique_device, 0) as impressions_unique_device,
        coalesce(ip.page_views, 0) as page_views,
        coalesce(ip.page_views_unique_device, 0) as page_views_unique_device,
        coalesce(dd.first_time_downloads, 0) as first_time_downloads,
        coalesce(dd.redownloads, 0) as redownloads,
        coalesce(dd.total_downloads, 0) as total_downloads,
        coalesce(sa.active_devices, 0) as active_devices,
        coalesce(id.deletions, 0) as deletions,
        coalesce(id.installations, 0) as installations,
        coalesce(sa.sessions, 0) as sessions
    from reporting_grain as rg
    left join app_crashes as ac 
        on rg.app_id = ac.app_id
        and rg.platform_version = ac.platform_version
        and rg.date_day = ac.date_day
        and rg.source_type = ac.source_type
        and rg.source_relation = ac.source_relation
    left join impressions_and_page_views as ip 
        on rg.app_id = ip.app_id
        and rg.platform_version = ip.platform_version
        and rg.date_day = ip.date_day
        and rg.source_type = ip.source_type
        and rg.source_relation = ip.source_relation
    left join downloads_daily as dd
        on rg.app_id = dd.app_id
        and rg.platform_version = dd.platform_version
        and rg.date_day = dd.date_day
        and rg.source_type = dd.source_type
        and rg.source_relation = dd.source_relation
    left join install_deletions as id
        on rg.app_id = id.app_id
        and rg.platform_version = id.platform_version
        and rg.date_day = id.date_day
        and rg.source_type = id.source_type
        and rg.source_relation = id.source_relation
    left join sessions_activity as sa
        on rg.app_id = sa.app_id
        and rg.platform_version = sa.platform_version
        and rg.date_day = sa.date_day
        and rg.source_type = sa.source_type
        and rg.source_relation = sa.source_relation
    left join app as a
        on rg.app_id = a.app_id
        and rg.source_relation = a.source_relation
)

select *
from final