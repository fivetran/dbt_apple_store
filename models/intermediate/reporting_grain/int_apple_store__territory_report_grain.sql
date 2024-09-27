with app_store_territory as (

    select *
    from {{ var('app_store_territory') }}
),

final as (
    select distinct
        source_relation,
        date_day,
        app_id,
        source_type,
        territory 
    from app_store_territory
)

select *
from final