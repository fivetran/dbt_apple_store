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
        sum(sessions) AS sessions,
        sum(unique_devices) AS active_devices,
        sum(distinct
            case when date_day between {{ dbt.dateadd('day', -30, 'date_day') }} and date_day then unique_devices end)
        as active_devices_last_30_days
    from base
    {{ dbt_utils.group_by(13) }}

)

select * 
from aggregated