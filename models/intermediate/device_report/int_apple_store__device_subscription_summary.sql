{{ config(enabled=var('apple_store__using_subscriptions', False)) }}

select
    app_name,
    date_day,
    device,
    source_type,
    source_relation,
    sum(active_free_trial_introductory_offer_subscriptions) as active_free_trial_introductory_offer_subscriptions,
    sum(active_pay_as_you_go_introductory_offer_subscriptions) as active_pay_as_you_go_introductory_offer_subscriptions,
    sum(active_pay_up_front_introductory_offer_subscriptions) as active_pay_up_front_introductory_offer_subscriptions,
    sum(active_standard_price_subscriptions) as active_standard_price_subscriptions
from {{ var('sales_subscription_summary') }}
{{ dbt_utils.group_by(5) }}