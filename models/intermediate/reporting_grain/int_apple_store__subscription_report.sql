{{ config(enabled=var('apple_store__using_subscriptions', False)) }}

with date_spine as (
    select
        date_day 
    from {{ ref('int_apple_store__date_spine') }}
), 

subscription_summary as (
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
),

-- Ensuring distinct combinations of all dimensions
distinct_reporting_grain as (
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

reporting_grain as (
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
    cross join distinct_reporting_grain as ug
)

select *
from reporting_grain
