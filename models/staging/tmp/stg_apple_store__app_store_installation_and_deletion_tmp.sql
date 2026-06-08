{% if var('apple_store_union_schemas', []) | length > 0 or var('apple_store_union_databases', []) | length > 0 %}

{{
    fivetran_utils.union_data(
        table_identifier='app_store_installation_and_deletion_standard_daily', 
        database_variable='apple_store_database', 
        schema_variable='apple_store_schema', 
        default_database=target.database,
        default_schema='apple_store',
        default_variable='app_store_installation_and_deletion_standard_daily',
        union_schema_variable='apple_store_union_schemas',
        union_database_variable='apple_store_union_databases'
    )
}}

{% else %}

{{
    fivetran_utils.union_connections(
        connection_dictionary='apple_store_sources',
        single_source_name='apple_store',
        single_table_name='app_store_installation_and_deletion_standard_daily'
    )
}}

{% endif %}