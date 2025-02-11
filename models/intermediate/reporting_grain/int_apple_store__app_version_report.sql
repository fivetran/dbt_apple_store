with app_crashes as (
    select
        app_id,
        app_version,
        date_day,
        source_type,
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
        app_id, 
        app_version, 
        source_type, 
        source_relation 
    from app_crashes
    
    union all
    
    select 
        app_id, 
        app_version, 
        source_type, 
        source_relation 
    from install_deletions
    
    union all
    
    select 
        app_id, 
        app_version, 
        source_type, 
        source_relation 
    from sessions_activity
)

-- Ensuring distinct combinations of all dimensions
select distinct
    app_id,
    app_version,
    source_type,
    source_relation
from pre_reporting_grain
