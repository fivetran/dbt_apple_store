with app_store_platform_version as (
    
    select *
    from {{ var('app_store_platform_version') }}
),

crashes_platform_version as (
    
    select *
    from {{ ref('int_apple_store__platform_version') }}
),

reporting_grain_combined as (

    select
        source_relation,
        date_day,
        app_id,
        source_type,
        platform_version
    from app_store_platform_version
    union all
    select 
        source_relation,
        date_day,
        app_id,
        source_type,
        platform_version
    from crashes_platform_version
),

final as (

    select distinct *
    from reporting_grain_combined
)

select *
from final