with app_store_source_type as (

    select *
    from {{ ref('int_apple_store__app_store_source_type') }}
),

final as (

    select distinct
        source_relation,
        date_day,
        app_id,
        source_type
    from app_store_source_type
)

select *
from final