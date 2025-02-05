with base as (

    select *
    from {{ var('app_store_download_detailed_daily') }}
),

aggregated as (

    select
        date_day,
        app_id,
        download_type,
        app_version,
        device,
        platform_version,
        source_type,
        page_type,
        pre_order,
        territory,
        counts,
        source_info,
        page_title,
        source_relation, 
        sum(case when lower(download_type) = 'first-time download' then counts else 0 end) as first_time_downloads,
        sum(case when lower(download_type) = 'redownload' then counts else 0 end) as redownloads,
        sum(case when lower(download_type) in ('first-time download','redownload') then counts else 0 end) as total_downloads,
    from base
    {{ dbt_utils.group_by(14) }}

)

select * 
from aggregated