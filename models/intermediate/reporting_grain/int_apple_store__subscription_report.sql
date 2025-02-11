with subscription_summary as (

    select
        vendor_number,
        app_apple_id,
        app_name,
        date_day,
        subscription_name,
        country,
        state,
        source_relation,
        sum(active_free_trial_introductory_offer_subscriptions) as active_free_trial_introductory_offer_subscriptions,
        sum(active_pay_as_you_go_introductory_offer_subscriptions) as active_pay_as_you_go_introductory_offer_subscriptions,
        sum(active_pay_up_front_introductory_offer_subscriptions) as active_pay_up_front_introductory_offer_subscriptions,
        sum(active_standard_price_subscriptions) as active_standard_price_subscriptions
    from {{ var('sales_subscription_summary') }}
    {{ dbt_utils.group_by(8) }}
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
        vendor_number,
        app_apple_id,
        app_name,
        date_day,
        subscription_name,
        country,
        state,
        source_relation
        {% for event_val in var('apple_store__subscription_events') %}
        , sum(case when lower(event) = '{{ event_val | trim | lower }}' then quantity else 0 end) as {{ 'event_' ~ event_val | replace(' ', '_') | trim | lower }}
        {% endfor %}
    from subscription_events_filtered
    {{ dbt_utils.group_by(8) }}
),

country_codes as (
    
    select * 
    from {{ var('apple_store_country_codes') }}
),

-- Unifying all dimension values before aggregation
pre_reporting_grain as (
    select 
        date_day, 
        vendor_number, 
        app_apple_id, 
        app_name, 
        subscription_name, 
        country, 
        state, 
        source_relation
    from subscription_summary

    union all

    select 
        date_day, 
        vendor_number, 
        app_apple_id, 
        app_name, 
        subscription_name, 
        country, 
        state, 
        source_relation
    from subscription_events
)

-- Ensuring distinct combinations of all dimensions
select distinct
    date_day,
    vendor_number,
    app_apple_id,
    app_name,
    subscription_name,
    country,
    state,
    source_relation
from pre_reporting_grain