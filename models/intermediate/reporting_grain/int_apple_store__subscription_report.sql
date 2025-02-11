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