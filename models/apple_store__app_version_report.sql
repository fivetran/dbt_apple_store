with app as (
    select
        app_id,
        app_name,
        source_relation
    from {{ ref('stg_apple_store__app_store_app') }}
),

app_crashes as (
    select * 
    from {{ ref('int_apple_store__app_version_app_crashes') }}
),

install_deletions as (
    select *
    from {{ ref('int_apple_store__app_version_install_deletions') }}
),

sessions_activity as (
    select *
    from {{ ref('int_apple_store__app_version_sessions_activity') }}
),

reporting_grain as (
    select *
    from {{ ref('int_apple_store__app_version_report') }}
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
    from reporting_grain as rg
    left join app_crashes as ac
        on rg.date_day = ac.date_day
        and rg.app_id = ac.app_id
        and rg.app_version = ac.app_version
        and rg.source_type = ac.source_type
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