with base as (

    select *
    from {{ var('sales_subscription_summary') }}
)

, app as (
    
    select *
    from {{ var('app') }}
)

, sales_account as (
    
    select * 
    from {{ var('sales_account') }}
)

, joined as (

    select 
        base.date_day,
        base.account_id,
        sales_account.account_name,
        app.app_id,
        base.app_name,
        subscription_name,
        country,
        case
            when state is null or trim(state) = '' then 'Not Available' else state
          end as state,
        active_free_trial_introductory_offer_subscriptions,
        active_pay_as_you_go_introductory_offer_subscriptions,
        active_pay_up_front_introductory_offer_subscriptions,
        active_standard_price_subscriptions
    from base
    left join app on base.app_name = app.app_name
    left join sales_account on base.account_id = sales_account.account_id
)

select * from joined