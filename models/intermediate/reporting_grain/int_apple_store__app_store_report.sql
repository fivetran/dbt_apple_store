with app_store as (

    select *
    from {{ ref('int_apple_store__app_store_overview') }}
),

final as (

    select distinct
        source_relation,
        date_day,
        app_id 
    from app_store
)

select *
from final