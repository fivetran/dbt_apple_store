with app as (
    select
        app_id,
        app_name,
        source_relation
    from {{ ref('stg_apple_store__app_store_app') }}
),

app_crashes as (
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

reporting_grain as (
    select *
    from {{ ref('int_apple_store__platform_version_report') }}
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