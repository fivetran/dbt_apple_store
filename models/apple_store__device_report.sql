with app as (
    select
        app_id,
        app_name,
        source_relation
    from {{ var('app_store_app') }}
),

app_store_device as (
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
    from {{ ref('int_apple_store__app_store_discovery_and_engagement_detailed_daily') }}
    group by 1,2,3,4,5
),

downloads_device as (
    select
        app_id,
        date_day,
        source_type,
        device,
        source_relation,
        sum(first_time_downloads) as first_time_downloads,
        sum(redownloads) as redownloads,
        sum(total_downloads) as total_downloads
    from {{ ref('int_apple_store__app_store_download_detailed_daily') }}
    group by 1,2,3,4,5
),

usage_device as (
    select
        app_id,
        date_day,
        source_type,
        device,
        source_relation,
        sum(installations) as installations,
        sum(deletions) as deletions
    from {{ ref('int_apple_store__app_store_installation_and_deletion_detailed_daily') }}
    group by 1,2,3,4,5
),

sessions_device as (
    select
        app_id,
        date_day,
        source_type,
        device,
        source_relation,
        sum(sessions) as sessions,
        sum(active_devices) as active_devices,
        sum(active_devices_last_30_days) as active_devices_last_30_days
    from {{ ref('int_apple_store__app_session_detailed_daily') }}
    group by 1,2,3,4,5
),

crashes_device as (
    select
        app_id,
        date_day,
        device,
        cast(null as {{ dbt.type_string() }}) as source_type,
        source_relation,
        sum(crashes) as crashes
    from {{ var('app_crash_daily') }}
    group by 1,2,3,4,5
),

{% if var('apple_store__using_subscriptions', False) %}
subscription as (

    select *
    from {{ var('sales_subscription_summary') }}
),
{% endif %}

-- union s all unique dimension values
pre_reporting_grain as (
    select date_day, app_id, source_type, device, source_relation from app_store_device
    union all
    select date_day, app_id, source_type, device, source_relation from downloads_device
    union all
    select date_day, app_id, source_type, device, source_relation from usage_device
    union all
    select date_day, app_id, source_type, device, source_relation from sessions_device
    union all
    select date_day, app_id, null as source_type, device, source_relation from crashes_device
),

-- ensures distinct combinations of all dimensions
reporting_grain as (
    select distinct
        date_day,
        app_id,
        source_type,
        device,
        source_relation
    from pre_reporting_grain
),

-- final aggregation using reporting grain
final as (
    select
        rg.source_relation,
        rg.date_day,
        rg.app_id,
        a.app_name,
        rg.source_type,
        rg.device,
        coalesce(asd.impressions, 0) as impressions,
        coalesce(asd.impressions_unique_device, 0) as impressions_unique_device,
        coalesce(asd.page_views, 0) as page_views,
        coalesce(asd.page_views_unique_device, 0) as page_views_unique_device,
        coalesce(cd.crashes, 0) as crashes,
        coalesce(dd.first_time_downloads, 0) as first_time_downloads,
        coalesce(dd.redownloads, 0) as redownloads,
        coalesce(dd.total_downloads, 0) as total_downloads,
        coalesce(sd.active_devices, 0) as active_devices,
        coalesce(sd.active_devices_last_30_days, 0) as active_devices_last_30_days,
        coalesce(ud.deletions, 0) as deletions,
        coalesce(ud.installations, 0) as installations,
        coalesce(sd.sessions, 0) as sessions

        {% if var('apple_store__using_subscriptions', False) %}
        ,
        coalesce(subscription.active_free_trial_introductory_offer_subscriptions, 0) as active_free_trial_introductory_offer_subscriptions,
        coalesce(subscription.active_pay_as_you_go_introductory_offer_subscriptions, 0) as active_pay_a_you_go_introductory_offer_subscriptions,
        coalesce(subscription.active_pay_up_front_introductory_offer_subscriptions, 0) as active_pay_up_front_introductory_offer_subscriptions,
        coalesce(subscription.active_standard_price_subscriptions, 0) as active_standard_price_subscriptions
        {% for event_val in var('apple_store__subscription_events') %}
        {% set event_column = 'event_' ~ event_val | replace(' ', '_') | trim | lower %}
        , coalesce({{ 'subscription.' ~ event_column }}, 0)
            as {{ event_column }} 
        {% endfor %}
        {% endif %}

    from reporting_grain rg
    left join app_store_device asd 
        on rg.app_id = asd.app_id
        and rg.date_day = asd.date_day
        and rg.source_type = asd.source_type
        and rg.device = asd.device
        and rg.source_relation = asd.source_relation
    left join crashes_device cd 
        on rg.app_id = cd.app_id
        and rg.date_day = cd.date_day
        and rg.device = cd.device
        and rg.source_relation = cd.source_relation
    left join downloads_device dd 
        on rg.app_id = dd.app_id
        and rg.date_day = dd.date_day
        and rg.source_type = dd.source_type
        and rg.device = dd.device
        and rg.source_relation = dd.source_relation
    left join usage_device ud 
        on rg.app_id = ud.app_id
        and rg.date_day = ud.date_day
        and rg.source_type = ud.source_type
        and rg.device = ud.device
        and rg.source_relation = ud.source_relation
    left join sessions_device sd 
        on rg.app_id = sd.app_id
        and rg.date_day = sd.date_day
        and rg.source_type = sd.source_type
        and rg.device = sd.device
        and rg.source_relation = sd.source_relation
    left join app a 
        on rg.app_id = a.app_id
        and rg.source_relation = a.source_relation

    {% if var('apple_store__using_subscriptions', False) %}
    left join subscription
        on reporting_grain.date_day = subscription.date_day
        and reporting_grain.source_relation = subscription.source_relation
        and a.app_name = subscription.app_name 
        and reporting_grain.source_type = subscription.source_type
        and reporting_grain.device = subscription.device
    {% endif %}
)

select *
from final