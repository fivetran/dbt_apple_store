with crashes_app_version_report as (
    
    select *
    from {{ ref('int_apple_store__crashes_app_version') }}
),

usage_app_version_report as (

    select *
    from {{ var('usage_app_version') }}
),

reporting_grain_combined as (
    
    select
        source_relation,
        date_day,
        app_id,
        source_type,
        app_version
    from usage_app_version_report
    union all 
    select 
        source_relation,
        date_day,
        app_id,
        source_type,
        app_version
    from crashes_app_version_report
),

final as (

    select distinct *
    from reporting_grain_combined
)

select *
from final