with date_spine as (
    select
        date_day 
    from {{ ref('int_apple_store__date_spine') }}
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
),

-- Ensuring distinct combinations of all dimensions
distinct_reporting_grain as (
    select distinct
        app_id,
        app_version,
        source_type,
        source_relation
    from pre_reporting_grain
),

reporting_grain as (
    select
        ds.date_day,
        ug.app_id,
        ug.app_version,
        ug.source_type, 
        ug.source_relation
    from date_spine as ds
    cross join distinct_reporting_grain as ug
)

select * 
from reporting_grain