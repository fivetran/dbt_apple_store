with app as (
    select
        app_id,
        app_name,
        source_relation
    from {{ var('app_store_app') }}
),

impressions_and_page_views as (
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

reporting_grain as (
    select *
    from {{ ref('int_apple_store__device_report') }}
),

-- Final aggregation using reporting grain
final as (
    select
        rg.source_relation,
        rg.date_day,
        rg.app_id,
        a.app_name,
        rg.source_type,
        rg.device,
        coalesce(ip.impressions, 0) as impressions,
        coalesce(ip.impressions_unique_device, 0) as impressions_unique_device,
        coalesce(ip.page_views, 0) as page_views,
        coalesce(ip.page_views_unique_device, 0) as page_views_unique_device,
        coalesce(ac.crashes, 0) as crashes,
        coalesce(dd.first_time_downloads, 0) as first_time_downloads,
        coalesce(dd.redownloads, 0) as redownloads,
        coalesce(dd.total_downloads, 0) as total_downloads,
        coalesce(sa.active_devices, 0) as active_devices,
        coalesce(id.deletions, 0) as deletions,
        coalesce(id.installations, 0) as installations,
        coalesce(sa.sessions, 0) as sessions

        {% if var('apple_store__using_subscriptions', False) %}
        ,
        coalesce(ss.active_free_trial_introductory_offer_subscriptions, 0) as active_free_trial_introductory_offer_subscriptions,
        coalesce(ss.active_pay_as_you_go_introductory_offer_subscriptions, 0) as active_pay_as_you_go_introductory_offer_subscriptions,
        coalesce(ss.active_pay_up_front_introductory_offer_subscriptions, 0) as active_pay_up_front_introductory_offer_subscriptions,
        coalesce(ss.active_standard_price_subscriptions, 0) as active_standard_price_subscriptions
        {% for event_val in var('apple_store__subscription_events') %}
        {% set event_column = 'event_' ~ event_val | replace(' ', '_') | trim | lower %}
        , coalesce({{ 'se.' ~ event_column }}, 0)
            as {{ event_column }} 
        {% endfor %}
        {% endif %}

    from reporting_grain as rg
    left join impressions_and_page_views as ip
        on rg.app_id = ip.app_id
        and rg.date_day = ip.date_day
        and rg.source_type = ip.source_type
        and rg.device = ip.device
        and rg.source_relation = ip.source_relation
    left join app_crashes as ac
        on rg.app_id = ac.app_id
        and rg.date_day = ac.date_day
        and rg.source_type = ac.source_type
        and rg.device = ac.device
        and rg.source_relation = ac.source_relation
    left join downloads_daily as dd 
        on rg.app_id = dd.app_id
        and rg.date_day = dd.date_day
        and rg.source_type = dd.source_type
        and rg.device = dd.device
        and rg.source_relation = dd.source_relation
    left join install_deletions as id 
        on rg.app_id = id.app_id
        and rg.date_day = id.date_day
        and rg.source_type = id.source_type
        and rg.device = id.device
        and rg.source_relation = id.source_relation
    left join sessions_activity as sa
        on rg.app_id = sa.app_id
        and rg.date_day = sa.date_day
        and rg.source_type = sa.source_type
        and rg.device = sa.device
        and rg.source_relation = sa.source_relation
    left join app as a
        on rg.app_id = a.app_id
        and rg.source_relation = a.source_relation

    {% if var('apple_store__using_subscriptions', False) %}
    left join subscription_summary as ss
        on rg.date_day = ss.date_day
        and rg.source_relation = ss.source_relation
        and a.app_name = ss.app_name 
        and rg.source_type = ss.source_type
        and rg.device = ss.device
    left join subscription_events as se
        on rg.date_day = se.date_day
        and rg.source_relation = se.source_relation
        and a.app_name = se.app_name 
        and rg.source_type = se.source_type
        and rg.device = se.device
    {% endif %}
)

select *
from final