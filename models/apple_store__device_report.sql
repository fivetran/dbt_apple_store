with app as (

    select * 
    from {{ var('app') }}
),

app_store_device_report as (

    select *
    from {{ var('app_store_device_report') }}
),

downloads_device_report as (

    select *
    from {{ var('downloads_device_report') }}
),

usage_device_report as (

    select *
    from {{ var('usage_device_report') }}
),

crashes_device_report as (

    select *
    from {{ ref('int_apple_store__crashes_device_report') }}
),

{% if var('apple_store__using_subscriptions', False) %}
subscription_device_report as (

    select *
    from {{ ref('int_apple_store__subscription_device_report') }}
),
{% endif %}

reporting_grain_combined as (

    select
        date_day,
        app_id,
        source_type,
        device 
    from app_store_device_report
    union all
    select
        date_day,
        app_id,
        source_type,
        device
    from crashes_device_report
),

reporting_grain as (
    
    select
        distinct *
    from reporting_grain_combined
),

joined as (

    select 
        reporting_grain.date_day,
        reporting_grain.app_id, 
        app.app_name,
        reporting_grain.source_type,
        reporting_grain.device,
        coalesce(app_store_device_report.impressions, 0) as impressions,
        coalesce(app_store_device_report.impressions_unique_device, 0) as impressions_unique_device,
        coalesce(app_store_device_report.page_views, 0) as page_views,
        coalesce(app_store_device_report.page_views_unique_device, 0) as page_views_unique_device,
        coalesce(crashes_device_report.crashes, 0) as crashes,
        coalesce(downloads_device_report.first_time_downloads, 0) as first_time_downloads,
        coalesce(downloads_device_report.redownloads, 0) as redownloads,
        coalesce(downloads_device_report.total_downloads, 0) as total_downloads,
        
        {% if var('apple_store__using_subscriptions', False) %}
        coalesce(subscription_device_report.event_subscribe,0) as event_subscribe,
        coalesce(subscription_device_report.event_cancel,0) as event_cancel,
        coalesce(subscription_device_report.event_renew,0) as event_renew,
        {% endif %}

        coalesce(usage_device_report.active_devices, 0) as active_devices,
        coalesce(usage_device_report.active_devices_last_30_days, 0) as active_devices_last_30_days,
        coalesce(usage_device_report.deletions, 0) as deletions,
        coalesce(usage_device_report.installations, 0) as installations,
        coalesce(usage_device_report.sessions, 0) as sessions
    from reporting_grain
    left join app 
        on reporting_grain.app_id = app.app_id
    left join app_store_device_report 
        on reporting_grain.date_day = app_store_device_report.date_day
        and reporting_grain.app_id = app_store_device_report.app_id 
        and reporting_grain.source_type = app_store_device_report.source_type
        and reporting_grain.device = app_store_device_report.device
    left join crashes_device_report
        on reporting_grain.date_day = crashes_device_report.date_day
        and reporting_grain.app_id = crashes_device_report.app_id
        and reporting_grain.source_type = crashes_device_report.source_type
        and reporting_grain.device = crashes_device_report.device
    left join downloads_device_report
        on reporting_grain.date_day = downloads_device_report.date_day
        and reporting_grain.app_id = downloads_device_report.app_id 
        and reporting_grain.source_type = downloads_device_report.source_type
        and reporting_grain.device = downloads_device_report.device
    {% if var('apple_store__using_subscriptions', False) %}
    left join subscription_device_report
        on reporting_grain.date_day = subscription_device_report.date_day
        and reporting_grain.app_id = subscription_device_report.app_id 
        and reporting_grain.source_type = subscription_device_report.source_type
        and reporting_grain.device = subscription_device_report.device
    {% endif %}
    left join usage_device_report
        on reporting_grain.date_day = usage_device_report.date_day
        and reporting_grain.app_id = usage_device_report.app_id 
        and reporting_grain.source_type = usage_device_report.source_type
        and reporting_grain.device = usage_device_report.device
)

select * from joined