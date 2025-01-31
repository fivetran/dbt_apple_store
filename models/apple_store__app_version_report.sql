with app as (
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
        cast(null as {{ dbt.type_string() }}) as source_type,
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
    from {{ ref('int_apple_store__app_store_installation_and_deletion_detailed_daily') }}
    group by 1,2,3,4,5
),

app_sessions as (
    select
        date_day,
        app_id,
        app_version,
        source_type,
        source_relation,
        sum(sessions) as sessions,
        sum(active_devices) as active_devices,
        sum(active_devices_last_30_days) as active_devices_last_30_days
    from {{ ref('int_apple_store__app_session_detailed_daily') }}
    group by 1,2,3,4,5
),

-- pre-reporting grain: unions all unique dimension values
pre_reporting_grain as (
    select date_day, app_id, app_version, source_type, source_relation from app_crashes
    union all
    select date_day, app_id, app_version, source_type, source_relation from install_deletions
    union all
    select date_day, app_id, app_version, source_type, source_relation from app_sessions
),

-- reporting grain: ensures distinct combinations of all dimensions
reporting_grain as (
    select distinct
        date_day,
        app_id,
        app_version,
        source_type,
        source_relation
    from pre_reporting_grain
),

-- final aggregation using reporting grain
final as (
    select
        rg.source_relation,
        rg.date_day,
        rg.app_id,
        a.app_name,
        rg.source_type,
        rg.app_version,
        coalesce(c.crashes, 0) as crashes,
        coalesce(s.active_devices, 0) as active_devices,
        coalesce(s.active_devices_last_30_days, 0) as active_devices_last_30_days,
        coalesce(u.deletions, 0) as deletions,
        coalesce(u.installations, 0) as installations,
        coalesce(s.sessions, 0) as sessions
    from reporting_grain rg
    left join app_crashes c
        on rg.date_day = c.date_day
        and rg.app_id = c.app_id
        and rg.app_version = c.app_version
        and rg.source_relation = c.source_relation
    left join install_deletions u
        on rg.date_day = u.date_day
        and rg.app_id = u.app_id
        and rg.app_version = u.app_version
        and rg.source_type = u.source_type
        and rg.source_relation = u.source_relation
    left join app_sessions s
        on rg.date_day = s.date_day
        and rg.app_id = s.app_id
        and rg.app_version = s.app_version
        and rg.source_type = s.source_type
        and rg.source_relation = s.source_relation
    left join app a
        on rg.app_id = a.app_id
        and rg.source_relation = a.source_relation
)

select *
from final
order by date_day, app_id, app_version
