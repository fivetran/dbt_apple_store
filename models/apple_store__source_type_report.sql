with app_store_source_type_report as (

    select *
    from {{ ref('int_apple_store__app_store_source_type_report') }}
),

downloads_source_type_report as (

    select *
    from {{ ref('int_apple_store__downloads_source_type_report') }}
),

usage_source_type_report as (

    select *
    from {{ ref('int_apple_store__usage_source_type_report') }}
),

app as (

    select * 
    from {{ var('app') }}
),

reporting_grain as (

    select distinct
        date_day,
        app_id,
        source_type
    from app_store_source_type_report
),

joined as (

    select 
        reporting_grain.date_day,
        reporting_grain.app_id, 
        app.app_name,
        reporting_grain.source_type,
        coalesce(app_store_source_type_report.impressions, 0) as impressions,
        coalesce(app_store_source_type_report.impressions_unique_device, 0) as impressions_unique_device,
        coalesce(app_store_source_type_report.page_views, 0) as page_views,
        coalesce(app_store_source_type_report.page_views_unique_device, 0) as page_views_unique_device,
        coalesce(downloads_source_type_report.first_time_downloads, 0) as first_time_downloads,
        coalesce(downloads_source_type_report.redownloads, 0) as redownloads,
        coalesce(downloads_source_type_report.total_downloads, 0) as total_downloads,
        coalesce(usage_source_type_report.active_devices, 0) as active_devices,
        coalesce(usage_source_type_report.active_devices_last_30_days, 0) as active_devices_last_30_days,
        coalesce(usage_source_type_report.deletions, 0) as deletions,
        coalesce(usage_source_type_report.installations, 0) as installations,
        coalesce(usage_source_type_report.sessions, 0) as sessions
    from reporting_grain
    left join app 
        on reporting_grain.app_id = app.app_id
    left join app_store_source_type_report
        on reporting_grain.date_day = app_store_source_type_report.date_day
        and reporting_grain.app_id = app_store_source_type_report.app_id 
        and reporting_grain.source_type = app_store_source_type_report.source_type
    left join downloads_source_type_report
        on reporting_grain.date_day = downloads_source_type_report.date_day
        and reporting_grain.app_id = downloads_source_type_report.app_id 
        and reporting_grain.source_type = downloads_source_type_report.source_type
    left join usage_source_type_report
        on reporting_grain.date_day = usage_source_type_report.date_day
        and reporting_grain.app_id = usage_source_type_report.app_id 
        and reporting_grain.source_type = usage_source_type_report.source_type
)

select * from joined