with date_spine as (
    select
        date_day 
    from {{ ref('int_apple_store__date_spine') }}
),

app as (
    select
        app_id,
        source_relation
    from {{ var('app_store_app') }}
),

-- Unifying all dimension values before aggregation
reporting_grain as (
    select
        ds.date_day,
        app.app_id,
        app.source_relation
    from date_spine as ds
    cross join app as app
)

select *
from reporting_grain