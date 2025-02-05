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
        app_version,
        date_day,
        '' as source_type,
        source_relation,
        sum(crashes) as crashes
    from {{ var('app_crash_daily') }}
    group by 1,2,3,4,5
),

install_deletions as (
    select
        app_id,
        app_version,
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
        date_day,
        app_id,
        app_version,
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
        date_day, 
        app_id, 
        app_version, 
        source_type, 
        source_relation 
    from app_crashes
    
    union all
    
    select 
        date_day, 
        app_id, 
        app_version, 
        source_type, 
        source_relation 
    from install_deletions
    
    union all
    
    select 
        date_day, 
        app_id, 
        app_version, 
        source_type, 
        source_relation 
    from sessions_activity
),

-- Ensuring distinct combinations of all dimensions
reporting_grain as (
    select distinct
        date_day,
        app_id,
        app_version,
        source_type,
        source_relation
    from pre_reporting_grain
),

reporting_grain_date_join as (
    select
        ds.date_day,
        ug.app_id,
        ug.app_version,
        coalesce(ug.source_type, '') as source_type, 
        ug.source_relation
    from date_spine as ds
    left join reporting_grain as ug
        on ds.date_day = ug.date_day
),

-- Final aggregation using reporting grain
final as (
    select
        rg.source_relation,
        rg.date_day,
        rg.app_id,
        a.app_name,
        rg.source_type,
        rg.app_version,
        coalesce(ac.crashes, 0) as crashes,
        coalesce(sa.active_devices, 0) as active_devices,
        coalesce(id.deletions, 0) as deletions,
        coalesce(id.installations, 0) as installations,
        coalesce(sa.sessions, 0) as sessions
    from reporting_grain_date_join as rg
    left join app_crashes as ac
        on rg.date_day = ac.date_day
        and rg.app_id = ac.app_id
        and rg.app_version = ac.app_version
        and coalesce(rg.source_type, '') = ac.source_type
        and rg.source_relation = ac.source_relation
    left join install_deletions as id
        on rg.date_day = id.date_day
        and rg.app_id = id.app_id
        and rg.app_version = id.app_version
        and rg.source_type = id.source_type
        and rg.source_relation = id.source_relation
    left join sessions_activity as sa
        on rg.date_day = sa.date_day
        and rg.app_id = sa.app_id
        and rg.app_version = sa.app_version
        and rg.source_type = sa.source_type
        and rg.source_relation = sa.source_relation
    left join app as a
        on rg.app_id = a.app_id
        and rg.source_relation = a.source_relation
)

select *
from final