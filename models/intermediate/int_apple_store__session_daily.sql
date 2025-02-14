with base as (

    select *
    from {{ var('app_session_standard_daily') }}
),

aggregated as (

    select 
        date_day,
        app_id,
        app_version,
        device,
        platform_version,
        source_type,
        page_type,
        app_download_date,
        territory,
        total_session_duration,
        source_relation,
        sum(sessions) as sessions,
        sum(unique_devices) as active_devices
    from base
    {{ dbt_utils.group_by(11) }}

)

select * 
from aggregated