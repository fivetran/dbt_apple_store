with impressions_and_page_views as (
    select
        app_id,
        date_day,
        source_type,
        device,
        source_relation,
        sum(impressions) as impressions,
        sum(impressions_unique_device) as impressions_unique_device,
        sum(page_views) as page_views,
        sum(page_views_unique_device) as page_views_unique_device
    from {{ ref('int_apple_store__discovery_and_engagement_daily') }}
    {{ dbt_utils.group_by(5) }}
),

downloads_daily as (
    select
        app_id,
        date_day,
        source_type,
        device,
        source_relation,
        sum(first_time_downloads) as first_time_downloads,
        sum(redownloads) as redownloads,
        sum(total_downloads) as total_downloads
    from {{ ref('int_apple_store__download_daily') }}
    {{ dbt_utils.group_by(5) }}
),

install_deletions as (
    select
        app_id,
        date_day,
        source_type,
        device,
        source_relation,
        sum(installations) as installations,
        sum(deletions) as deletions
    from {{ ref('int_apple_store__installation_and_deletion_daily') }}
    {{ dbt_utils.group_by(5) }}
),

sessions_activity as (
    select
        app_id,
        date_day,
        source_type,
        device,
        source_relation,
        sum(sessions) as sessions,
        sum(active_devices) as active_devices
    from {{ ref('int_apple_store__session_daily') }}
    {{ dbt_utils.group_by(5) }}
),

app_crashes as (
    select
        app_id,
        date_day,
        device,
        source_type,
        source_relation,
        sum(crashes) as crashes
    from {{ var('app_crash_daily') }}
    {{ dbt_utils.group_by(5) }}
),

{% if var('apple_store__using_subscriptions', False) %}
subscription_summary as (

    select
        app_name,
        date_day,
        device,
        source_type,
        source_relation,
        sum(active_free_trial_introductory_offer_subscriptions) as active_free_trial_introductory_offer_subscriptions,
        sum(active_pay_as_you_go_introductory_offer_subscriptions) as active_pay_as_you_go_introductory_offer_subscriptions,
        sum(active_pay_up_front_introductory_offer_subscriptions) as active_pay_up_front_introductory_offer_subscriptions,
        sum(active_standard_price_subscriptions) as active_standard_price_subscriptions
    from {{ var('sales_subscription_summary') }}
    {{ dbt_utils.group_by(5) }}
),

subscription_events_filtered as (

    select *
    from {{ var('sales_subscription_events') }} 
    where lower(event)
        in (
            {% for event_val in var('apple_store__subscription_events') %}
                {% if loop.index0 != 0 %}
                , 
                {% endif %}
                '{{ var("apple_store__subscription_events")[loop.index0] | trim | lower }}'
            {% endfor %}   
        )
),

subscription_events as (
    
    select
        app_name,
        date_day,
        device,
        source_type,
        source_relation
        {% for event_val in var('apple_store__subscription_events') %}
        , sum(case when lower(event) = '{{ event_val | trim | lower }}' then quantity else 0 end) as {{ 'event_' ~ event_val | replace(' ', '_') | trim | lower }}
        {% endfor %}
    from subscription_events_filtered
    {{ dbt_utils.group_by(5) }}
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