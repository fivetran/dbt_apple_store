{{ config(enabled=var('apple_store__using_subscriptions', False)) }}

with app as (

    select * 
    from {{ var('app') }}
), 

base as (

    select 
        date_day,
        device,
        app_name,
        event,
        quantity
    from {{ var('sales_subscription_events') }}
    where event in ('Renew', 'Cancel', 'Subscribe')
), 

base_with_events as (
    
    select 
        date_day,
        device,
        app_name, 
        'No Associated Source Type' as source_type,
        sum(case 
            when event = 'Subscribe' then quantity else 0
            end) as event_subscribe,
        sum(case 
            when event = 'Renew' then quantity else 0 
            end) as event_renew,
        sum(case 
            when event = 'Cancel' then quantity else 0
            end) as event_cancel
    from base
    {{ dbt_utils.group_by(4) }}
),

joined as (

    select 
        base_with_events.date_day,
        base_with_events.device,
        app.app_id,
        base_with_events.source_type,
        base_with_events.event_subscribe,
        base_with_events.event_renew,
        base_with_events.event_cancel
    from base_with_events 
    join app on base_with_events.app_name = app.app_name
)

select * from joined