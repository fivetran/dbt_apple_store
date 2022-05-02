with base as (

    select * 
    from {{ var('app_store_device') }}
),

aggregated as (

    select 
        date_day,
        app_id,
        source_type,
        sum(impressions) as impressions,
        sum(impressions_unique_device) as impressions_unique_device,
        sum(page_views) as page_views,
        sum(page_views_unique_device) as page_views_unique_device
    from base 
    {{ dbt_utils.group_by(3) }}

)

select * from aggregated