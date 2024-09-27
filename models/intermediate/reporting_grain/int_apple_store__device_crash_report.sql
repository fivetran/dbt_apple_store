with app_store_device as (

    select *
    from {{ var('app_store_device') }}
),

crashes_device as (

    select *
    from {{ ref('int_apple_store__crashes_device') }}
),

reporting_grain_combined as (

    select
        source_relation,
        date_day,
        app_id,
        source_type,
        device 
    from app_store_device
    union all
    select
        source_relation,
        date_day,
        app_id,
        source_type,
        device
    from crashes_device
),

final as (
    
    select distinct *
    from reporting_grain_combined
)

select *
from final