with base as (

    select *
    from {{ var('sales_subscription_event_summary') }}
)

, app as (
    
    select *
    from {{ var('app') }}
)

, sales_account as (
    
    select * 
    from {{ var('sales_account') }}
)

, filtered as (

    select *
    from base 
    where lower(event)
        in (
            {% for event_val in var('apple_store__subscription_events') %}
                {% if loop.index0 != 0 %}
                , '{{ var("apple_store__subscription_events")[loop.index0] | trim |lower }}'
                {% else %}
                '{{ var("apple_store__subscription_events")[loop.index0] | trim | lower }}'
                {% endif %}
            {% endfor %}   
        )
)

, pivoted as (
    
    select
        date_day
        , account_id
        , app_name
        , subscription_name
        , country
        , state
        {% for event_val in var('apple_store__subscription_events') %}
        , sum(case when lower(event) = '{{ event_val | trim | lower }}' then quantity else 0 end) as {{ 'event_' ~ event_val | replace(' ', '_') | trim | lower }}
        {% endfor %}
    from filtered
    {{ dbt_utils.group_by(6) }}
)

, joined as (

    select 
        pivoted.date_day
        , pivoted.account_id
        , sales_account.account_name
        , app.app_id
        , pivoted.app_name
        , subscription_name
        , country
        , case
            when state is null or trim(state) = '' then 'Not Available' else state
          end as state 
        {% for event_val in var('apple_store__subscription_events') %}
        , {{ 'event_' ~ event_val | replace(' ', '_') | trim | lower }}
        {% endfor %}
    from pivoted
    left join app 
        on pivoted.app_name = app.app_name
    left join sales_account 
        on pivoted.account_id = sales_account.account_id
)

select *
from joined