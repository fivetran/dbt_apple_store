with app_crashes as (
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
