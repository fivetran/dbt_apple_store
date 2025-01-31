with app as (
    select
        app_id,
        app_name,
        source_relation
    from {{ var('app_store_app') }}
),

impressions_and_page_views as (
    select
        app_id,
        date_day,
        source_relation,
        sum(impressions) as impressions,
        sum(page_views) as page_views
    from {{ ref('int_apple_store__app_store_discovery_and_engagement_detailed_daily') }}
    group by 1,2,3
),

crashes as (
    select
        app_id,
        date_day,
        source_relation,
        sum(crashes) as crashes
    from {{ var('app_crash_daily') }}
    group by 1,2,3
),

downloads as (
    select
        app_id,
        date_day,
        source_relation,
        sum(first_time_downloads) as first_time_downloads,
        sum(redownloads) as redownloads,
        sum(total_downloads) as total_downloads
    from {{ ref('int_apple_store__app_store_download_detailed_daily') }}
    group by 1,2,3
),

usage as (
    select
        app_id,
        date_day,
        source_relation,
        sum(installations) as installations,
        sum(deletions) as deletions
    from {{ ref('int_apple_store__app_store_installation_and_deletion_detailed_daily') }}
    group by 1,2,3
),

sessions as (
    select
        app_id,
        date_day,
        source_relation,
        sum(sessions) as sessions,
        sum(active_devices) as active_devices
    from {{ ref('int_apple_store__app_session_detailed_daily') }}
    group by 1,2,3
),

-- unions all unique dimension values
pre_reporting_grain as (
    select date_day, app_id, source_relation from impressions_and_page_views
    union all
    select date_day, app_id, source_relation from crashes
    union all
    select date_day, app_id, source_relation from downloads
    union all
    select date_day, app_id, source_relation from usage
    union all
    select date_day, app_id, source_relation from sessions
),

-- ensures distinct combinations of all dimensions
reporting_grain as (
    select distinct
        date_day,
        app_id,
        source_relation
    from pre_reporting_grain
),

-- final aggregation using reporting grain
final as (
    select
        rg.source_relation,
        rg.date_day,
        rg.app_id,
        app.app_name,
        coalesce(ip.impressions, 0) as impressions,
        coalesce(ip.page_views, 0) as page_views,
        coalesce(c.crashes, 0) as crashes,
        coalesce(d.first_time_downloads, 0) as first_time_downloads,
        coalesce(d.redownloads, 0) as redownloads,
        coalesce(d.total_downloads, 0) as total_downloads,
        coalesce(s.active_devices, 0) as active_devices,
        coalesce(u.deletions, 0) as deletions,
        coalesce(u.installations, 0) as installations,
        coalesce(s.sessions, 0) as sessions
        {% if var('apple_store__using_subscriptions', False) %}
        ,
        coalesce(subscriptions.active_free_trial_introductory_offer_subscriptions, 0) as active_free_trial_introductory_offer_subscriptions,
        coalesce(subscriptions.active_pay_as_you_go_introductory_offer_subscriptions, 0) as active_pay_as_you_go_introductory_offer_subscriptions,
        coalesce(subscriptions.active_pay_up_front_introductory_offer_subscriptions, 0) as active_pay_up_front_introductory_offer_subscriptions,
        coalesce(subscriptions.active_standard_price_subscriptions, 0) as active_standard_price_subscriptions
        {% for event_val in var('apple_store__subscription_events') %}
        {% set event_column = 'event_' ~ event_val | replace(' ', '_') | trim | lower %}
        , coalesce({{ 'subscriptions.' ~ event_column }}, 0)
            as {{ event_column }} 
        {% endfor %}
        {% endif %}
    from reporting_grain rg
    left join impressions_and_page_views ip 
        on rg.app_id = ip.app_id
        and rg.date_day = ip.date_day
        and rg.source_relation = ip.source_relation
    left join crashes c 
        on rg.app_id = c.app_id
        and rg.date_day = c.date_day
        and rg.source_relation = c.source_relation
    left join downloads d 
        on rg.app_id = d.app_id
        and rg.date_day = d.date_day
        and rg.source_relation = d.source_relation
    left join usage u 
        on rg.app_id = u.app_id
        and rg.date_day = u.date_day
        and rg.source_relation = u.source_relation
    left join sessions s 
        on rg.app_id = s.app_id
        and rg.date_day = s.date_day
        and rg.source_relation = s.source_relation
    left join app
        on rg.app_id = app.app_id
        and rg.source_relation = app.source_relation

    {% if var('apple_store__using_subscriptions', False) %}
    left join subscriptions 
        on reporting_grain.date_day = subscriptions.date_day
        and reporting_grain.source_relation = subscriptions.source_relation
        and reporting_grain.app_id = subscriptions.app_id
    {% endif %}
)

select *
from final