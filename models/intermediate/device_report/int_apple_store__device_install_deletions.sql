select
    app_id,
    date_day,
    source_type,
    device,
    source_relation,
    sum(installations) as installations,
    sum(deletions) as deletions
from {{ ref('int_apple_store__installation_and_deletion_daily') }}
{{ dbt_utils.group_by(5) }}