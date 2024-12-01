{% macro fabric__post_snapshot(staging_relation) %}
  -- Clean up the snapshot temp table
  {% do drop_relation_if_exists(staging_relation) %}
{% endmacro %}

{% macro fabric__create_columns(relation, columns) %}
  {% for column in columns %}
    {% call statement() %}
      alter table {{ relation.render() }} add "{{ column.name }}" {{ column.data_type }} NULL;
    {% endcall %}
  {% endfor %}
{% endmacro %}

{% macro fabric__get_true_sql() %}
  {{ return('1=1') }}
{% endmacro %}


{% macro fabric__build_snapshot_table(strategy, relation) %}

    select *,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        select * from {{ relation }}
    ) sbq

{% endmacro %}

{% macro fabric__snapshot_staging_table(strategy, temp_snapshot_relation, target_relation) -%}

    with snapshot_query as (

        select * from {{ temp_snapshot_relation }}

    ),

    snapshotted_data as (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation }}
        where dbt_valid_to is null

    ),

    insertions_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            {{ strategy.updated_at }} as dbt_valid_to

        from snapshot_query
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (

        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as dbt_valid_from,
            {{ snapshot_get_time() }} as dbt_updated_at,
            {{ snapshot_get_time() }} as dbt_valid_to,
            snapshotted_data.dbt_scd_id

        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}

{%- endmacro %}

{% macro build_snapshot_staging_table(strategy, temp_snapshot_relation, target_relation) %}
    {% set temp_relation = make_temp_relation(target_relation) %}
    {{ adapter.drop_relation(temp_relation) }}

    {% set select = snapshot_staging_table(strategy, temp_snapshot_relation, target_relation) %}

    {% set tmp_tble_vw_relation = temp_relation.incorporate(path={"identifier": temp_relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}
    -- Dropping temp view relation if it exists
    {{ adapter.drop_relation(tmp_tble_vw_relation) }}

    {% call statement('build_snapshot_staging_relation') %}
        {{ get_create_table_as_sql(True, temp_relation, select) }}
    {% endcall %}

    -- Dropping temp view relation if it exists
    {{ adapter.drop_relation(tmp_tble_vw_relation) }}

    {% do return(temp_relation) %}
{% endmacro %}
