with base as (

    select *
    from {{ var('app_session_detailed_daily') }}
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
        source_info,
        page_title,
        source_relation,
        sum(sessions) as sessions,
        sum(unique_devices) as active_devices
    from base
    {{ dbt_utils.group_by(13) }}

)

select * 
from aggregated