with base as (

    select *
    from {{ var('usage_device') }}
),

aggregated as (

    select 
        date_day,
        app_id,
        sum(active_devices) as active_devices,
        sum(active_devices_last_30_days) as active_devices_last_30_days,
        sum(deletions) as deletions,
        sum(installations) as installations,
        sum(sessions) as sessions
    from base
    {{ dbt_utils.group_by(2) }}
)

select * from aggregated