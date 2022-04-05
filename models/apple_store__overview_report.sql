with app_store as (

    select *
    from {{ ref('int_apple_store__app_store_overview_report') }}
),

crashes as (

    select *
    from {{ ref('int_apple_store__crashes_overview_report') }}
),

downloads as (

    select *
    from {{ ref('int_apple_store__downloads_overview_report') }}
),

subscriptions as (

    select *
    from {{ ref('int_apple_store__sales_subscription_overview') }}
), 

app as (

    select * 
    from {{ var('app') }}
),

reporting_grain as (

    select distinct
        date_day,
        app_id 
    from app_store
), 

joined as (

    select 
        reporting_grain.date_day,
        reporting_grain.app_id,
        app.app_name,
        app_store.impressions,
        app_store.impressions_unique_device,
        app_store.page_views,
        app_store.page_views_unique_device,
        crashes.crashes,
        downloads.first_time_downloads,
        downloads.redownloads,
        downloads.total_downloads,
        subscriptions.active_free_trial_introductory_offer_subscriptions,
        subscriptions.active_pay_as_you_go_introductory_offer_subscriptions,
        subscriptions.active_pay_up_front_introductory_offer_subscriptions
        {% for event_val in var('apple_store__subscription_events') %}
        {% set event_column = 'event_' ~ event_val | replace(' ', '_') | trim | lower %}
        , coalesce({{ 'subscriptions.' ~ event_column }}, 0)
            as {{ event_column }} 
        {% endfor %}
    from reporting_grain
    left join app 
        on reporting_grain.app_id = app.app_id
    left join app_store 
        on reporting_grain.date_day = app_store.date_day
        and reporting_grain.app_id = app_store.app_id
    left join crashes
        on reporting_grain.date_day = crashes.date_day
        and reporting_grain.app_id = crashes.app_id
    left join downloads
        on reporting_grain.date_day = downloads.date_day
        and reporting_grain.app_id = downloads.app_id
    left join subscriptions 
        on reporting_grain.date_day = subscriptions.date_day
        and reporting_grain.app_id = subscriptions.app_id
)

select * from joined