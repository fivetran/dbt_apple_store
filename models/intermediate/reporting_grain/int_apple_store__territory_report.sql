with impressions_and_page_views as (
    select * 
    from {{ ref('int_apple_store__territory_impressions_page_views') }}
),

downloads_daily as (
    select *
    from {{ ref('int_apple_store__territory_downloads_daily') }}
),

install_deletions as (
    select *
    from {{ ref('int_apple_store__territory_install_deletions') }}
),

sessions_activity as (
    select *
    from {{ ref('int_apple_store__territory_sessions_activity') }}
),

country_codes as (
    
    select * 
    from {{ var('apple_store_country_codes') }}
),

-- Unifying all dimension values before aggregation
pre_reporting_grain as (
    select
        app_id, 
        source_type, 
        territory, 
        source_relation 
    from impressions_and_page_views

    union all

    select
        app_id, 
        source_type, 
        territory, 
        source_relation 
    from downloads_daily

    union all

    select
        app_id, 
        source_type, 
        territory, 
        source_relation 
    from install_deletions

    union all

    select
        app_id, 
        source_type, 
        territory, 
        source_relation 
    from sessions_activity
)

-- Ensuring distinct combinations of all dimensions
select distinct
    app_id,
    source_type,
    territory,
    source_relation
from pre_reporting_grain