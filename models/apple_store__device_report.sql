with date_spine as (
    select
        date_day 
    from {{ ref('int_apple_store__date_spine') }}
),

app as (
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
        '' as source_type,
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
        '' as source_type,
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
        '' as source_type,
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
        date_day, 
        app_id, 
        source_type, 
        device, 
        source_relation 
    from impressions_and_page_views
    
    union all

    select 
        date_day, 
        app_id, 
        source_type, 
        device, 
        source_relation 
    from downloads_daily
    
    union all

    select 
        date_day, 
        app_id, 
        source_type, 
        device, 
        source_relation 
    from install_deletions
    
    union all

    select 
        date_day, 
        app_id, 
        source_type, 
        device, 
        source_relation 
    from sessions_activity
    
    union all

    select 
        date_day, 
        app_id, 
        source_type, 
        device, 
        source_relation 
    from app_crashes
),

-- Ensuring distinct combinations of all dimensions
reporting_grain as (
    select distinct
        date_day,
        app_id,
        source_type,
        device,
        source_relation
    from pre_reporting_grain
),

reporting_grain_date_join as (
    select
        ds.date_day,
        ug.app_id,
        coalesce(ug.source_type, '') as source_type, 
        ug.device,
        ug.source_relation
    from date_spine as ds
    left join reporting_grain as ug
        on ds.date_day = ug.date_day
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

    from reporting_grain_date_join as rg
    left join impressions_and_page_views as ip
        on rg.app_id = ip.app_id
        and rg.date_day = ip.date_day
        and rg.source_type = ip.source_type
        and rg.device = ip.device
        and rg.source_relation = ip.source_relation
    left join app_crashes as ac
        on rg.app_id = ac.app_id
        and rg.date_day = ac.date_day
        and coalesce(rg.source_type, '') = ac.source_type
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
        and coalesce(rg.source_type, '') = ss.source_type
        and rg.device = ss.device
    left join subscription_events as se
        on rg.date_day = se.date_day
        and rg.source_relation = se.source_relation
        and a.app_name = se.app_name 
        and coalesce(rg.source_type, '') = se.source_type
        and rg.device = se.device
    {% endif %}
)

select *
from final