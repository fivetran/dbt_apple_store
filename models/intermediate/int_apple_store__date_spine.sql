{{ config(materialized='table') }}

-- depends_on: {{ ref('stg_apple_store__app_store_discovery_and_engagement_daily') }}
-- depends_on: {{ ref('stg_apple_store__app_crash_daily') }}
-- depends_on: {{ ref('stg_apple_store__app_store_download_daily') }}
-- depends_on: {{ ref('stg_apple_store__app_store_installation_and_deletion_daily') }}
-- depends_on: {{ ref('stg_apple_store__app_session_daily') }}

{% set first_date_query %}

    select min(date_day) as min_date_day
    from (
        select date_day from {{ ref('stg_apple_store__app_store_discovery_and_engagement_daily') }}
        union all
        select date_day from {{ ref('stg_apple_store__app_crash_daily') }}
        union all
        select date_day from {{ ref('stg_apple_store__app_store_download_daily') }}
        union all
        select date_day from {{ ref('stg_apple_store__app_store_installation_and_deletion_daily') }}
        union all
        select date_day from {{ ref('stg_apple_store__app_session_daily') }}
    ) as all_dates

{% endset %}

{%- set first_date = dbt_utils.get_single_value(first_date_query) %}

select
    cast(date_day as date) as date_day 
from (
        {{
            dbt_utils.date_spine(
                datepart="day",
                start_date = "cast('" ~ first_date ~ "' as date)",
                end_date=dbt.dateadd("day", 1, dbt.current_timestamp())
            )   
        }} 
    ) as date_spine
