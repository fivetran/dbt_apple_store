{{ config(enabled=var('apple_store__using_subscriptions', False)) }}

{% if var('apple_store_union_schemas', []) | length > 0 or var('apple_store_union_databases', []) | length > 0 %}

{{
    fivetran_utils.union_data(
        table_identifier='sales_subscription_event_summary', 
        database_variable='apple_store_database', 
        schema_variable='apple_store_schema', 
        default_database=target.database,
        default_schema='apple_store',
        default_variable='sales_subscription_events',
        union_schema_variable='apple_store_union_schemas',
        union_database_variable='apple_store_union_databases'
    )
}}

{% else %}

{{
    fivetran_utils.union_connections(
        connection_dictionary='apple_store_sources',
        single_source_name='apple_store',
        single_table_name='sales_subscription_event_summary'
    )
}}

{% endif %}