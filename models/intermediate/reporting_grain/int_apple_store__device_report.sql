with impressions_and_page_views as (
    select *
    from {{ ref('int_apple_store__device_impressions_page_views') }}
),

downloads_daily as (
    select *
    from {{ ref('int_apple_store__device_downloads_daily') }}
),

install_deletions as (
    select *
    from {{ ref('int_apple_store__device_install_deletions') }}
),

sessions_activity as (
    select *
    from {{ ref('int_apple_store__device_sessions_activity') }}
),

app_crashes as (
    select * 
    from {{ ref('int_apple_store__device_app_crashes') }}
),

{% if var('apple_store__using_subscriptions', False) %}
subscription_summary as (
    select *
    from {{ ref('int_apple_store__device_subscription_summary') }}
),

subscription_events as (
    select *
    from {{ ref('int_apple_store__device_subscription_events') }}
),

{% endif %}

-- Unifying all dimension values before aggregation
pre_reporting_grain as (
    select 
        app_id, 
        source_type, 
        device, 
        source_relation 
    from impressions_and_page_views
    
    union all

    select 
        app_id, 
        source_type, 
        device, 
        source_relation 
    from downloads_daily
    
    union all

    select 
        app_id, 
        source_type, 
        device, 
        source_relation 
    from install_deletions
    
    union all

    select 
        app_id, 
        source_type, 
        device, 
        source_relation 
    from sessions_activity
    
    union all

    select 
        app_id, 
        source_type, 
        device, 
        source_relation 
    from app_crashes
)

-- Ensuring distinct combinations of all dimensions
select distinct
    app_id,
    source_type,
    device,
    source_relation
from pre_reporting_grain