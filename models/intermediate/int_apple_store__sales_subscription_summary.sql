{{ config(enabled=var('apple_store__using_subscriptions', True)) }}

with base as (

    select *
    from {{ var('sales_subscription_summary') }}
),

app as (
    
    select *
    from {{ var('app') }}
),

sales_account as (
    
    select * 
    from {{ var('sales_account') }}
),

joined as (

    select 
        base.date_day,
        base.account_id,
        sales_account.account_name,
        app.app_id,
        base.app_name,
        base.subscription_name,
        base.country,
        case
            when base.state is null or trim(base.state) = '' then 'Not Available' else base.state
          end as state,
        base.active_free_trial_introductory_offer_subscriptions,
        base.active_pay_as_you_go_introductory_offer_subscriptions,
        base.active_pay_up_front_introductory_offer_subscriptions,
        base.active_standard_price_subscriptions
    from base
    left join app 
        on base.app_name = app.app_name
    left join sales_account 
        on base.account_id = sales_account.account_id
)

select * from joined