with app_store_territory_report as (

    select *
    from {{ var('app_store_territory_report') }}
),

downloads_territory_report as (

    select *
    from {{ var('downloads_territory_report') }}
),

usage_territory_report as (

    select * 
    from {{ var('usage_territory_report') }}
),

app as (

    select * 
    from {{ var('app') }}
),

reporting_grain as (

    select distinct
        date_day,
        app_id,
        source_type,
        territory 
    from app_store_territory_report
),

joined as (

    select 
        reporting_grain.date_day,
        reporting_grain.app_id,
        app.app_name,
        reporting_grain.source_type,
        reporting_grain.territory,
        coalesce(app_store_territory_report.impressions, 0) as impressions,
        coalesce(app_store_territory_report.impressions_unique_device, 0) as impressions_unique_device,
        coalesce(app_store_territory_report.page_views, 0) as page_views,
        coalesce(app_store_territory_report.page_views_unique_device, 0) as page_views_unique_device,
        coalesce(downloads_territory_report.first_time_downloads, 0) as first_time_downloads,
        coalesce(downloads_territory_report.redownloads, 0) as redownloads,
        coalesce(downloads_territory_report.total_downloads, 0) as total_downloads,
        coalesce(usage_territory_report.active_devices, 0) as active_devices,
        coalesce(usage_territory_report.active_devices_last_30_days, 0) as active_devices_last_30_days,
        coalesce(usage_territory_report.deletions, 0) as deletions,
        coalesce(usage_territory_report.installations, 0) as installations,
        coalesce(usage_territory_report.sessions, 0) as sessions
    from reporting_grain
    left join app 
        on reporting_grain.app_id = app.app_id
    left join app_store_territory_report 
        on reporting_grain.date_day = app_store_territory_report.date_day
        and reporting_grain.app_id = app_store_territory_report.app_id 
        and reporting_grain.source_type = app_store_territory_report.source_type
        and reporting_grain.territory = app_store_territory_report.territory
    left join downloads_territory_report
        on reporting_grain.date_day = downloads_territory_report.date_day
        and reporting_grain.app_id = downloads_territory_report.app_id 
        and reporting_grain.source_type = downloads_territory_report.source_type
        and reporting_grain.territory = downloads_territory_report.territory
    left join usage_territory_report
        on reporting_grain.date_day = usage_territory_report.date_day
        and reporting_grain.app_id = usage_territory_report.app_id 
        and reporting_grain.source_type = usage_territory_report.source_type
        and reporting_grain.territory = usage_territory_report.territory
)

select * from joined