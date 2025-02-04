{{ config(enabled=var('apple_store__using_subscriptions', False)) }}

with date_spine as (
    select
        date_day 
    from {{ ref('int_apple_store__date_spine') }}
),

subscription_summary as (

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
),

-- Ensuring distinct combinations of all dimensions
reporting_grain as (
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
),

reporting_grain_date_join as (
    select
        ds.date_day,
        ug.vendor_number,
        ug.app_apple_id,
        ug.app_name,
        ug.subscription_name,
        ug.country,
        ug.state,
        ug.source_relation
    from date_spine as ds
    cross join reporting_grain as ug
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
    from reporting_grain_date_join as rg
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