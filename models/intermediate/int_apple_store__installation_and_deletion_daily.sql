with base as (

    select *
    from {{ ref('stg_apple_store__app_store_installation_and_deletion_daily')}}

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
        app_download_date,
        territory,
        source_relation,
        sum(case when lower(download_type) = 'first-time download' then counts else 0 end) as first_time_downloads,
        sum(case when lower(download_type) = 'redownload' then counts else 0 end) as redownloads,
        sum(case when lower(download_type) in ('first-time download','redownload') then counts else 0 end) as total_downloads,
        sum(case when lower(event) = 'delete' then counts else 0 end) as deletions,
        sum(case when lower(event) = 'install' then counts else 0 end) as installations
    from base
    {{ dbt_utils.group_by(11) }}

)

select * 
from aggregated