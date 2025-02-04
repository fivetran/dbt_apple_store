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
        date_day, 
        app_id, 
        source_type, 
        source_relation 
    from impressions_and_page_views

    union all

    select 
        date_day, 
        app_id, 
        source_type, 
        source_relation 
    from install_deletions

    union all

    select 
        date_day, 
        app_id, 
        source_type, 
        source_relation 
    from sessions_activity
),

-- Ensuring distinct combinations of all dimensions
reporting_grain as (
    select distinct
        date_day,
        app_id,
        source_type,
        source_relation
    from pre_reporting_grain
),

reporting_grain_date_join as (
    select
        ds.date_day,
        ug.app_id,
        ug.source_type,
        ug.source_relation
    from date_spine as ds
    cross join reporting_grain as ug
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
    from reporting_grain_date_join as rg
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
