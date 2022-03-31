with base as (

    select *
    from {{ var('crashes_app_version_device_report') }}
)

, aggregated as (

    select 
        date_day, 
        app_id,
        device,
        sum(crashes) as crashes
    from base
    {{ dbt_utils.group_by(3) }}
)

select *
from aggregated