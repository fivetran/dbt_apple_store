with subscription_summary as (

    select *
    from {{ ref('int_apple_store__sales_subscription_summary') }}
)

, subscription_events as (

    select *
    from {{ ref('int_apple_store__sales_subscription_event_summary') }}
)

, reporting_grain as (

    select
        cast(date_day as date) as date_day,
        account_id,
        account_name,
        app_name,
        app_id,
        subscription_name,
        country,
        state 
    from subscription_summary
    union 
    select
        cast(date_day as date) as date_day,
        account_id,
        account_name,
        app_name,
        app_id,
        subscription_name,
        country,
        state 
    from subscription_events
)

, joined as (

    select 
        reporting_grain.date_day,
        reporting_grain.account_id,
        reporting_grain.account_name, 
        reporting_grain.app_id,
        reporting_grain.app_name,
        reporting_grain.subscription_name, 
        reporting_grain.country,
        reporting_grain.state,
        {% for event_val in var('apple_store__subscription_events') %}
        {% set event_column = 'event_' ~ event_val | replace(' ', '_') | trim | lower %}
        coalesce({{ 'subscription_events.' ~ event_column }}, 0)
            as {{ event_column }}, 
        {% endfor %}
        subscription_summary.active_free_trial_introductory_offer_subscriptions,
        subscription_summary.active_pay_as_you_go_introductory_offer_subscriptions,
        subscription_summary.active_pay_up_front_introductory_offer_subscriptions,
        subscription_summary.active_standard_price_subscriptions
    from reporting_grain
    left join subscription_summary
        on reporting_grain.date_day = subscription_summary.date_day
        and reporting_grain.account_id =  subscription_summary.account_id 
        and reporting_grain.app_name = subscription_summary.app_name
        and reporting_grain.subscription_name = subscription_summary.subscription_name
        and reporting_grain.country = subscription_summary.country
        and reporting_grain.state = subscription_summary.state
    left join subscription_events
        on reporting_grain.date_day = subscription_events.date_day
        and reporting_grain.account_id =  subscription_events.account_id 
        and reporting_grain.app_name = subscription_events.app_name
        and reporting_grain.subscription_name = subscription_events.subscription_name
        and reporting_grain.country = subscription_events.country
        and reporting_grain.state = subscription_events.state
)

select * from joined
