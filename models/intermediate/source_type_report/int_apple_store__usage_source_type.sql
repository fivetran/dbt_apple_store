with base as (

    select *
    from {{ var('usage_device') }}
),

aggregated as (

    select 
        date_day,
        app_id,
        source_type,
        sum(active_devices) as active_devices,
        sum(deletions) as deletions,
        sum(installations) as installations,
        sum(sessions) as sessions
    from base
    {{ dbt_utils.group_by(3) }}
)

select * from aggregated