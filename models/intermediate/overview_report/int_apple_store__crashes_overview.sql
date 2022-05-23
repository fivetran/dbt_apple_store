with base as (

    select *
    from {{ var('crashes_app_version') }}
),

aggregated as (

    select 
        date_day, 
        app_id,
        sum(crashes) as crashes
    from base
    {{ dbt_utils.group_by(2) }}
)

select * 
from aggregated