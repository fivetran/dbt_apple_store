with base as (
    
    select * 
    from {{ var('downloads_device_report') }}
),

aggregated as (

    select 
        date_day,
        app_id,
        sum(first_time_downloads) as first_time_downloads,
        sum(redownloads) as redownloads,
        sum(total_downloads) as total_downloads
    from base 
    {{ dbt_utils.group_by(2) }}
)

select * from aggregated