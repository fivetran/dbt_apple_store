with base as (

    select *
    from {{ var('crashes_platform_version') }}
),

aggregated as (

    select 
        date_day, 
        app_id,
        platform_version,
        cast(null as {{ dbt.type_string() }}) as source_type,
        sum(crashes) as crashes
    from base
    {{ dbt_utils.group_by(4) }}
)

select * 
from aggregated