{{ config(enabled=var('apple_store__using_subscriptions', False)) }}

with subscription_events_filtered as (

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
)

select *
from subscription_events