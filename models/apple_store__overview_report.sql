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
    from {{ ref('int_apple_store__app_store_discovery_and_engagement_daily') }}
    group by 1,2,3
),

app_crashes as (
    select
        app_id,
        date_day,
        source_relation,
        sum(crashes) as crashes
    from {{ var('app_crash_daily') }}
    group by 1,2,3
),

downloads_daily as (
    select
        app_id,
        date_day,
        source_relation,
        sum(first_time_downloads) as first_time_downloads,
        sum(redownloads) as redownloads,
        sum(total_downloads) as total_downloads
    from {{ ref('int_apple_store__app_store_download_daily') }}
    group by 1,2,3
),

install_deletions as (
    select
        app_id,
        date_day,
        source_relation,
        sum(installations) as installations,
        sum(deletions) as deletions
    from {{ ref('int_apple_store__app_store_installation_and_deletion_daily') }}
    group by 1,2,3
),

sessions_activity as (
    select
        app_id,
        date_day,
        source_relation,
        sum(sessions) as sessions,
        sum(active_devices) as active_devices
    from {{ ref('int_apple_store__app_session_daily') }}
    group by 1,2,3
),

{% if var('apple_store__using_subscriptions', False) %}
subscription_summary as (

    select
        app_name,
        date_day,
        source_relation,
        sum(active_free_trial_introductory_offer_subscriptions) as active_free_trial_introductory_offer_subscriptions,
        sum(active_pay_as_you_go_introductory_offer_subscriptions) as active_pay_as_you_go_introductory_offer_subscriptions,
        sum(active_pay_up_front_introductory_offer_subscriptions) as active_pay_up_front_introductory_offer_subscriptions,
        sum(active_standard_price_subscriptions) as active_standard_price_subscriptions
    from {{ var('sales_subscription_summary') }}
    {{ dbt_utils.group_by(3) }}
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
        source_relation
        {% for event_val in var('apple_store__subscription_events') %}
        , sum(case when lower(event) = '{{ event_val | trim | lower }}' then quantity else 0 end) as {{ 'event_' ~ event_val | replace(' ', '_') | trim | lower }}
        {% endfor %}
    from subscription_events_filtered
    {{ dbt_utils.group_by(3) }}
),

{% endif %}

-- Unifying all dimension values before aggregation
pre_reporting_grain as (
    select date_day, app_id, source_relation from impressions_and_page_views
    union all
    select date_day, app_id, source_relation from crashes
    union all
    select date_day, app_id, source_relation from downloads
    union all
    select date_day, app_id, source_relation from usage
    union all
    select date_day, app_id, source_relation from sessions_activity
),

-- Ensuring distinct combinations of all dimensions
reporting_grain as (
    select distinct
        date_day,
        app_id,
        source_relation
    from pre_reporting_grain
),

-- Final aggregation using reporting grain
final as (
    select
        rg.source_relation,
        rg.date_day,
        rg.app_id,
        a.app_name,
        coalesce(ip.impressions, 0) as impressions,
        coalesce(ip.page_views, 0) as page_views,
        coalesce(ac.crashes, 0) as crashes,
        coalesce(d.first_time_downloads, 0) as first_time_downloads,
        coalesce(d.redownloads, 0) as redownloads,
        coalesce(d.total_downloads, 0) as total_downloads,
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
    from reporting_grain rg
    left join impressions_and_page_views ip 
        on rg.app_id = ip.app_id
        and rg.date_day = ip.date_day
        and rg.source_relation = ip.source_relation
    left join app_crashes ac
        on rg.app_id = ac.app_id
        and rg.date_day = ac.date_day
        and rg.source_relation = ac.source_relation
    left join downloads_daily dd 
        on rg.app_id = dd.app_id
        and rg.date_day = dd.date_day
        and rg.source_relation = dd.source_relation
    left join install_deletions id 
        on rg.app_id = id.app_id
        and rg.date_day = id.date_day
        and rg.source_relation = id.source_relation
    left join sessions_activity sa 
        on rg.app_id = sa.app_id
        and rg.date_day = sa.date_day
        and rg.source_relation = sa.source_relation
    left join app a
        on rg.app_id = a.app_id
        and rg.source_relation = a.source_relation

    {% if var('apple_store__using_subscriptions', False) %}
    left join subscription_summary ss 
        on rg.date_day = ss.date_day
        and rg.source_relation = ss.source_relation
        and rg.app_name = ss.app_name
    left join subscription_events se 
        on rg.date_day = se.date_day
        and rg.source_relation = se.source_relation
        and rg.app_name = se.app_name
    {% endif %}
)

select *
from final