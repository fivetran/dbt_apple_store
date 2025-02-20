{{ config(materialized='table') }}

with spine as (

    {% if execute and flags.WHICH in ('run', 'build') %}

{% set first_date_query %}

    select min(date_day) as min_date_day
    from (
        select cast(date as date) as date_day from {{ source('apple_store', 'app_store_installation_and_deletion_standard_daily') }}
        union all
        select cast(date as date) as date_day from {{ source('apple_store', 'app_store_discovery_and_engagement_standard_daily') }}
        union all
        select cast(date as date) as date_day from {{ source('apple_store', 'app_store_download_standard_daily') }}
        union all
        select cast(date as date) as date_day from {{ source('apple_store', 'app_crash_daily') }}
        union all
        select cast(date as date) as date_day from {{ source('apple_store', 'app_session_standard_daily') }}
    ) as all_dates

{% endset %}

{%- set first_date = dbt_utils.get_single_value(first_date_query) %}

{% else %}
{%- set first_date = '2024-01-01' %}

{% endif %}

{{
    dbt_utils.date_spine(
        datepart="day",
        start_date = "cast('" ~ first_date ~ "' as date)",
        end_date=dbt.dateadd("day", 1, dbt.current_timestamp())
    )   
}} 

)

select
    cast(date_day as date) as date_day 
from spine

