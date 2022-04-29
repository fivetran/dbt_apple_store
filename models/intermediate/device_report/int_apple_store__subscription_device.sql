{{ config(enabled=var('apple_store__using_subscriptions', False)) }}

with app as (
    
    select *
    from {{ var('app') }}
),

subscription_summary as (

    select
        date_day,
        app_name,
        device,
        sum(active_free_trial_introductory_offer_subscriptions) as active_free_trial_introductory_offer_subscriptions,
        sum(active_pay_as_you_go_introductory_offer_subscriptions) as active_pay_as_you_go_introductory_offer_subscriptions,
        sum(active_pay_up_front_introductory_offer_subscriptions) as active_pay_up_front_introductory_offer_subscriptions,
        sum(active_standard_price_subscriptions) as active_standard_price_subscriptions
    from {{ var('sales_subscription_summary') }}
    {{ dbt_utils.group_by(3) }}
), 

filtered_subscription_events as (

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

pivoted_subscription_events as (
    
    select
        date_day
        , app_name
        , device
        {% for event_val in var('apple_store__subscription_events') %}
        , coalesce(sum(case when lower(event) = '{{ event_val | trim | lower }}' then quantity else 0 end),0) as {{ 'event_' ~ event_val | replace(' ', '_') | trim | lower }}
        {% endfor %}
    from filtered_subscription_events
    {{ dbt_utils.group_by(3) }}
),

joined as (

    select 
        app.app_id,
        pivoted_subscription_events.*,
        subscription_summary.active_free_trial_introductory_offer_subscriptions,
        subscription_summary.active_pay_as_you_go_introductory_offer_subscriptions,
        subscription_summary.active_pay_up_front_introductory_offer_subscriptions,
        subscription_summary.active_standard_price_subscriptions,
        'No Associated Source Type' as source_type
    from subscription_summary 
    left join pivoted_subscription_events
        on subscription_summary.date_day = pivoted_subscription_events.date_day
        and subscription_summary.app_name = pivoted_subscription_events.app_name
        and subscription_summary.device = pivoted_subscription_events.device
    left join app 
        on subscription_summary.app_name = app.app_name
    
)

select * from joined