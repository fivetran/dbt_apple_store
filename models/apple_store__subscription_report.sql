{{ config(enabled=var('apple_store__using_subscriptions', False)) }}

with app as (
    select
        app_id,
        app_name,
        source_relation
    from {{ var('app_store_app') }}
),

subscription_summary as (
    select
        app_apple_id as app_id,
        date_day,
        subscription_name,
        country,
        state,
        source_relation,
        sum(active_free_trial_introductory_offer_subscriptions) as active_free_trial_introductory_offer_subscriptions,
        sum(active_pay_as_you_go_introductory_offer_subscriptions) as active_pay_as_you_go_introductory_offer_subscriptions,
        sum(active_pay_up_front_introductory_offer_subscriptions) as active_pay_up_front_introductory_offer_subscriptions,
        sum(active_standard_price_subscriptions) as active_standard_price_subscriptions
    from {{ var('stg_apple_store__sales_subscription_summary') }}
    group by 1,2,3,4,5,6
),

subscription_events as (
    select
        app_apple_id as app_id,
        date_day,
        subscription_name,
        country,
        state,
        source_relation,
        event,
        sum(quantity) as event_count
    from {{ var('stg_apple_store__sales_subscription_events') }}
    group by 1,2,3,4,5,6,7
),

-- pre-reporting grain: unions all unique dimension values
pre_reporting_grain as (
    select date_day, app_id, subscription_name, country, state, source_relation from subscription_summary
    union all
    select date_day, app_id, subscription_name, country, state, source_relation from subscription_events
),

-- reporting grain: ensures distinct combinations of all dimensions
reporting_grain as (
    select distinct
        date_day,
        app_id,
        subscription_name,
        country,
        state,
        source_relation
    from pre_reporting_grain
),

-- pivot subscription events dynamically
-- subscription_events_pivoted as (
    
-- ),

-- final aggregation using reporting grain
final as (
    select
        rg.date_day,
        rg.app_id,
        a.app_name,
        rg.subscription_name,
        rg.country as territory_short,
        rg.state,
        rg.source_relation,
        -- Placeholder for country code mapping
        'placeholder for territory_long' as territory_long,
        'placeholder for region' as region,
        'placeholder for sub_region' as sub_region,
        coalesce(ss.active_free_trial_introductory_offer_subscriptions, 0) as active_free_trial_introductory_offer_subscriptions,
        coalesce(ss.active_pay_as_you_go_introductory_offer_subscriptions, 0) as active_pay_as_you_go_introductory_offer_subscriptions,
        coalesce(ss.active_pay_up_front_introductory_offer_subscriptions, 0) as active_pay_up_front_introductory_offer_subscriptions,
        coalesce(ss.active_standard_price_subscriptions, 0) as active_standard_price_subscriptions,
        se.*
    from reporting_grain rg
    left join app a 
        on rg.app_id = a.app_id
        and rg.source_relation = a.source_relation
    left join subscription_summary ss 
        on rg.app_id = ss.app_id
        and rg.date_day = ss.date_day
        and rg.subscription_name = ss.subscription_name
        and rg.country = ss.country
        and rg.state = ss.state
        and rg.source_relation = ss.source_relation
    left join subscription_events se 
        on rg.app_id = se.app_id
        and rg.date_day = se.date_day
        and rg.subscription_name = se.subscription_name
        and rg.country = se.country
        and rg.state = se.state
        and rg.source_relation = se.source_relation
    -- left join subscription_events_pivoted se 
    --     on rg.app_id = se.app_id
    --     and rg.date_day = se.date_day
    --     and rg.subscription_name = se.subscription_name
    --     and rg.country = se.country
    --     and rg.state = se.state
    --     and rg.source_relation = se.source_relation
)

select *
from final