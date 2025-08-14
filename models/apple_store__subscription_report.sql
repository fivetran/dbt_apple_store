{{ config(enabled=var('apple_store__using_subscriptions', False)) }}

with subscription_summary as (
    select * 
    from {{ ref('int_apple_store__subscription_summary') }}
),

subscription_events as (
    select *
    from {{ ref('int_apple_store__subscription_events') }}
),

country_codes as (
    
    select * 
    from {{ ref('apple_store_country_codes') }}
),

reporting_grain as (
    select *
    from {{ ref('int_apple_store__subscription_report') }}
),

-- Final aggregation using reporting grain
final as (
    select
        rg.date_day,
        rg.vendor_number,
        rg.app_apple_id,
        rg.app_name,
        rg.subscription_name,
        case 
            when country_codes.alternative_country_name is null then country_codes.country_name
            else country_codes.alternative_country_name
        end as territory_long,
        rg.country as territory_short,
        rg.state,
        country_codes.region, 
        country_codes.sub_region,
        rg.source_relation,
        coalesce(ss.active_free_trial_introductory_offer_subscriptions, 0) as active_free_trial_introductory_offer_subscriptions,
        coalesce(ss.active_pay_as_you_go_introductory_offer_subscriptions, 0) as active_pay_as_you_go_introductory_offer_subscriptions,
        coalesce(ss.active_pay_up_front_introductory_offer_subscriptions, 0) as active_pay_up_front_introductory_offer_subscriptions,
        coalesce(ss.active_standard_price_subscriptions, 0) as active_standard_price_subscriptions
        {% for event_val in var('apple_store__subscription_events') %}
        {% set event_column = 'event_' ~ event_val | replace(' ', '_') | trim | lower %}
        , coalesce({{ 'se.' ~ event_column }}, 0)
            as {{ event_column }} 
        {% endfor %}
    from reporting_grain as rg
    left join subscription_summary as ss 
        on rg.vendor_number = ss.vendor_number
        and rg.app_apple_id = ss.app_apple_id
        and rg.date_day = ss.date_day
        and rg.subscription_name = ss.subscription_name
        and rg.country = ss.country
        and rg.state = ss.state
        and rg.source_relation = ss.source_relation
    left join subscription_events as se
        on rg.vendor_number = ss.vendor_number
        and rg.app_apple_id = se.app_apple_id
        and rg.date_day = se.date_day
        and rg.subscription_name = se.subscription_name
        and rg.country = se.country
        and rg.state = se.state
        and rg.source_relation = se.source_relation
    left join country_codes
        on rg.country = country_codes.country_code_alpha_2
)

select *
from final