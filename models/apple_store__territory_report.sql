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

impressions_and_page_views as (
    select * 
    from {{ ref('int_apple_store__territory_impressions_page_views') }}
),

downloads_daily as (
    select *
    from {{ ref('int_apple_store__territory_downloads_daily') }}
),

install_deletions as (
    select *
    from {{ ref('int_apple_store__territory_install_deletions') }}
),

sessions_activity as (
    select *
    from {{ ref('int_apple_store__territory_sessions_activity') }}
),

country_codes as (
    
    select * 
    from {{ var('apple_store_country_codes') }}
),

-- Ensuring distinct combinations of all dimensions
pre_reporting_grain as (
    select *
    from {{ ref('int_apple_store__territory_report') }}
),

reporting_grain as (
    select
        ds.date_day,
        ug.app_id,
        ug.source_type,
        ug.territory,
        ug.source_relation
    from date_spine as ds
    cross join pre_reporting_grain as ug
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
        coalesce(id.deletions, 0) as deletions,
        coalesce(id.installations, 0) as installations,
        coalesce(sa.sessions, 0) as sessions
    from reporting_grain as rg
    left join app as a
        on rg.app_id = a.app_id
        and rg.source_relation = a.source_relation
    left join impressions_and_page_views as ip 
        on rg.app_id = ip.app_id
        and rg.date_day = ip.date_day
        and rg.source_type = ip.source_type
        and rg.territory = ip.territory
        and rg.source_relation = ip.source_relation
    left join downloads_daily as dd
        on rg.app_id = dd.app_id
        and rg.date_day = dd.date_day
        and rg.source_type = dd.source_type
        and rg.territory = dd.territory
        and rg.source_relation = dd.source_relation
    left join install_deletions as id
        on rg.app_id = id.app_id
        and rg.date_day = id.date_day
        and rg.source_type = id.source_type
        and rg.territory = id.territory
        and rg.source_relation = id.source_relation
    left join sessions_activity as sa
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