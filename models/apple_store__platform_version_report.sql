with app as (

    select * 
    from {{ var('app') }}
),

app_store_platform_version_report as (
    
    select *
    from {{ var('app_store_platform_version_report') }}
),

crashes_platform_version_report as (
    
    select *
    from {{ ref('int_apple_store__platform_version_report') }}
),

downloads_platform_version_report as (

    select *
    from {{ var('downloads_platform_version_report') }}
),

usage_platform_version_report as (

    select *
    from {{ var('usage_platform_version_report') }}
),

reporting_grain as (

    select
        date_day,
        app_id,
        source_type,
        platform_version
    from app_store_platform_version_report
    union 
    select 
        date_day,
        app_id,
        source_type,
        platform_version
    from crashes_platform_version_report
),

joined as (

    select 
        reporting_grain.date_day,
        reporting_grain.app_id, 
        app.app_name,
        reporting_grain.source_type,
        reporting_grain.platform_version,
        coalesce(app_store_platform_version_report.impressions, 0) as impressions,
        coalesce(app_store_platform_version_report.impressions_unique_device, 0) as impressions_unique_device,
        coalesce(app_store_platform_version_report.page_views, 0) as page_views,
        coalesce(app_store_platform_version_report.page_views_unique_device, 0) as page_views_unique_device,
        coalesce(crashes_platform_version_report.crashes, 0) as crashes,
        coalesce(downloads_platform_version_report.first_time_downloads, 0) as first_time_downloads,
        coalesce(downloads_platform_version_report.redownloads, 0) as redownloads,
        coalesce(downloads_platform_version_report.total_downloads, 0) as total_downloads,
        coalesce(usage_platform_version_report.active_devices, 0) as active_devices,
        coalesce(usage_platform_version_report.active_devices_last_30_days, 0) as active_devices_last_30_days,
        coalesce(usage_platform_version_report.deletions, 0) as deletions,
        coalesce(usage_platform_version_report.installations, 0) as installations,
        coalesce(usage_platform_version_report.sessions, 0) as sessions
    from reporting_grain
    left join app 
        on reporting_grain.app_id = app.app_id
    left join app_store_platform_version_report 
        on reporting_grain.date_day = app_store_platform_version_report.date_day
        and reporting_grain.app_id = app_store_platform_version_report.app_id 
        and reporting_grain.source_type = app_store_platform_version_report.source_type
        and reporting_grain.platform_version = app_store_platform_version_report.platform_version
    left join crashes_platform_version_report
        on reporting_grain.date_day = crashes_platform_version_report.date_day
        and reporting_grain.app_id = crashes_platform_version_report.app_id
        and reporting_grain.source_type = crashes_platform_version_report.source_type
        and reporting_grain.platform_version = crashes_platform_version_report.platform_version    
    left join downloads_platform_version_report
        on reporting_grain.date_day = downloads_platform_version_report.date_day
        and reporting_grain.app_id = downloads_platform_version_report.app_id 
        and reporting_grain.source_type = downloads_platform_version_report.source_type
        and reporting_grain.platform_version = downloads_platform_version_report.platform_version
    left join usage_platform_version_report
        on reporting_grain.date_day = usage_platform_version_report.date_day
        and reporting_grain.app_id = usage_platform_version_report.app_id 
        and reporting_grain.source_type = usage_platform_version_report.source_type
        and reporting_grain.platform_version = usage_platform_version_report.platform_version
)

select * from joined