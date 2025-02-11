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
    from {{ ref('int_apple_store__source_type_impressions_page_views') }}
),

install_deletions as (
    select * 
    from {{ ref('int_apple_store__source_type_install_deletions') }}
),

sessions_activity as (
    select * 
    from {{ ref('int_apple_store__source_type_sessions_activity') }}
),

pre_reporting_grain as (
    select *
    from {{ ref('int_apple_store__source_type_report') }}
),

reporting_grain as (
    select
        ds.date_day,
        ug.app_id,
        ug.source_type,
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
        coalesce(ip.impressions, 0) as impressions,
        coalesce(ip.page_views, 0) as page_views,
        coalesce(id.first_time_downloads, 0) as first_time_downloads,
        coalesce(id.redownloads, 0) as redownloads,
        coalesce(id.total_downloads, 0) as total_downloads,
        coalesce(id.deletions, 0) as deletions,
        coalesce(id.installations, 0) as installations,
        coalesce(sa.active_devices, 0) as active_devices,
        coalesce(sa.sessions, 0) as sessions
    from reporting_grain as rg
    left join impressions_and_page_views as ip
        on rg.date_day = ip.date_day
        and rg.app_id = ip.app_id
        and rg.source_type = ip.source_type
        and rg.source_relation = ip.source_relation
    left join install_deletions as id
        on rg.date_day = id.date_day 
        and rg.app_id = id.app_id
        and rg.source_type = id.source_type
        and rg.source_relation = id.source_relation
    left join sessions_activity as sa
        on rg.date_day = sa.date_day 
        and rg.app_id = sa.app_id 
        and rg.source_type = sa.source_type
        and rg.source_relation = sa.source_relation
    left join app as a
        on rg.app_id = a.app_id
        and rg.source_relation = a.source_relation
)

select *
from final
