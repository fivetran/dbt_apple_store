with base as (

    select *
    from {{ var('crashes_app_version') }}
),

aggregated as (

    select 
        date_day, 
        app_id,
        app_version,
        cast(null as {{ dbt_utils.type_string() }}) as source_type,
        sum(crashes) as crashes
    from base
    {{ dbt_utils.group_by(4) }}
)

select * 
from aggregated