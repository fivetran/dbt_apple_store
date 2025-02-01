with app as (
    select
        app_id,
        app_name,
        source_relation
    from {{ var('app_store_app') }}
),

impressions_and_page_views as (
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
        sum(active_devices) as active_devices,
        sum(active_devices_last_30_days) as active_devices_last_30_days
    from {{ ref('int_apple_store__session_daily') }}
    group by 1,2,3,4,5
),

country_codes as (
    
    select * 
    from {{ var('apple_store_country_codes') }}
),

-- Unifying all dimension values before aggregation
pre_reporting_grain as (
    select date_day, app_id, source_type, territory, source_relation from impressions_and_page_views
    union all
    select date_day, app_id, source_type, territory, source_relation from downloads_daily
    union all
    select date_day, app_id, source_type, territory, source_relation from install_deletions
    union all
    select date_day, app_id, source_type, territory, source_relation from sessions_activity
),

-- Ensuring distinct combinations of all dimensions
reporting_grain as (
    select distinct
        date_day,
        app_id,
        source_type,
        territory,
        source_relation
    from pre_reporting_grain
),

-- Final aggregation using reporting grain
final as (
    select
        rg.source_relation,
        rg.date_day,
        rg.app_id,
        a.app_name,
        rg.source_type,
        rg.territory as territory_long,
        coalesce(official_country_codes.country_code_alpha_2, alternative_country_codes.country_code_alpha_2) as territory_short,
        coalesce(official_country_codes.region, alternative_country_codes.region) as region,
        coalesce(official_country_codes.sub_region, alternative_country_codes.sub_region) as sub_region,
        coalesce(ip.impressions, 0) as impressions,
        coalesce(ip.impressions_unique_device, 0) as impressions_unique_device,
        coalesce(ip.page_views, 0) as page_views,
        coalesce(ip.page_views_unique_device, 0) as page_views_unique_device,
        coalesce(dd.first_time_downloads, 0) as first_time_downloads,
        coalesce(dd.redownloads, 0) as redownloads,
        coalesce(dd.total_downloads, 0) as total_downloads,
        coalesce(sa.active_devices, 0) as active_devices,
        coalesce(sa.active_devices_last_30_days, 0) as active_devices_last_30_days,
        coalesce(id.deletions, 0) as deletions,
        coalesce(id.installations, 0) as installations,
        coalesce(sa.sessions, 0) as sessions
    from reporting_grain rg
    left join app a 
        on rg.app_id = a.app_id
        and rg.source_relation = a.source_relation
    left join impressions_and_page_views ip 
        on rg.app_id = ip.app_id
        and rg.date_day = ip.date_day
        and rg.source_type = ip.source_type
        and rg.territory = ip.territory
        and rg.source_relation = ip.source_relation
    left join downloads_daily dd 
        on rg.app_id = dd.app_id
        and rg.date_day = dd.date_day
        and rg.source_type = dd.source_type
        and rg.territory = dd.territory
        and rg.source_relation = dd.source_relation
    left join install_deletions id 
        on rg.app_id = id.app_id
        and rg.date_day = id.date_day
        and rg.source_type = id.source_type
        and rg.territory = id.territory
        and rg.source_relation = id.source_relation
    left join sessions_activity sa
        on rg.app_id = sa.app_id
        and rg.date_day = sa.date_day
        and rg.source_type = sa.source_type
        and rg.territory = sa.territory
        and rg.source_relation = sa.source_relation
    left join country_codes as official_country_codes
        on rg.territory = official_country_codes.country_name
    left join country_codes as alternative_country_codes
        on rg.territory = alternative_country_codes.alternative_country_name
)

select *
from final