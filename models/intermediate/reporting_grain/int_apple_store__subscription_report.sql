{{ config(enabled=var('apple_store__using_subscriptions', False)) }}

with subscription_summary as (

    select *
    from {{ ref('int_apple_store__sales_subscription_summary') }}
),

subscription_events as (

    select *
    from {{ ref('int_apple_store__sales_subscription_events') }}
),

reporting_grain_combined as (

    select
        source_relation,
        cast(date_day as date) as date_day,
        account_id,
        account_name,
        app_name,
        app_id,
        subscription_name,
        country,
        state 
    from subscription_summary
    union all
    select
        source_relation,
        cast(date_day as date) as date_day,
        account_id,
        account_name,
        app_name,
        app_id,
        subscription_name,
        country,
        state 
    from subscription_events
),

final as (

    select distinct *
    from reporting_grain_combined
)

select *
from final