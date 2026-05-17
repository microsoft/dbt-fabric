  {# global project no longer includes semi-colons in merge statements, so
     default macro are invoked below w/ a semi-colons after it.
     more context:
     https://github.com/dbt-labs/dbt-core/pull/3510
     https://getdbt.slack.com/archives/C50NEBJGG/p1636045535056600
  #}

{% macro fabric__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) %}
  {{ default__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) }};
{% endmacro %}

{% macro fabric__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
  {{ default__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) }};
{% endmacro %}

{% macro fabric__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) %}

    {% set query_label = apply_label() %}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{ target }}
            where exists (
                select null
                from {{ source }}
                where
                {% for key in unique_key %}
                    {{ source }}.{{ key }} = {{ target }}.{{ key }}
                    {{ "and " if not loop.last }}
                {% endfor %}
            )
            {% if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {% endif %}
            {{ query_label }}
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            )
            {%- if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {%- endif -%}
            {{ query_label }}
        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    ){{ query_label }}
{% endmacro %}

{#
  Option A: MERGE with WHEN NOT MATCHED BY SOURCE THEN DELETE.
  Deletes target rows whose unique_key has no match in the source relation.
  Use when the incremental model returns the complete current dataset (not a delta).
  Config: delete_not_matched_by_source: true
#}
{% macro fabric__get_merge_delete_not_matched_sql(target, source, unique_key, dest_columns, incremental_predicates=none) %}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set query_label = apply_label() -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% do predicates.append("DBT_INTERNAL_SOURCE." ~ unique_key ~ " = DBT_INTERNAL_DEST." ~ unique_key) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on ({{ predicates | join(") and (") }})

    {% if unique_key %}
    when matched then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

    when not matched by source then delete
    {{ query_label }};
{% endmacro %}


{#
  Option B: Standard MERGE followed by a separate DELETE for soft-delete patterns.
  Deletes target rows that are present in the source and satisfy delete_condition.
  Use when the source carries a soft-delete flag column.
  Config: delete_condition: "DBT_INTERNAL_SOURCE.is_deleted = 1"
  Aliases available in the condition: DBT_INTERNAL_SOURCE, DBT_INTERNAL_DEST
#}
{% macro fabric__get_merge_delete_condition_sql(target, source, unique_key, delete_condition) %}
    {%- set query_label = apply_label() -%}

    delete DBT_INTERNAL_DEST
    from {{ target }} as DBT_INTERNAL_DEST
    inner join {{ source }} as DBT_INTERNAL_SOURCE
        on
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {{ "and " if not loop.last }}
            {% endfor %}
        {% else %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endif %}
    where {{ delete_condition }}
    {{ query_label }};
{% endmacro %}


{% macro fabric__get_incremental_microbatch_sql(arg_dict) %}
    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set incremental_predicates = [] if arg_dict.get('incremental_predicates') is none else arg_dict.get('incremental_predicates') -%}

    {#-- Add additional incremental_predicates to filter for batch --#}
    {% if model.config.get("__dbt_internal_microbatch_event_time_start") -%}
    {{ log("incremenal append event start time > DBT_INTERNAL_TARGET." ~ model.config.event_time ~ " >= '" ~ model.config.__dbt_internal_microbatch_event_time_start ~ "'") }}
        {% do incremental_predicates.append("DBT_INTERNAL_TARGET." ~ model.config.event_time ~ " >= '" ~ model.config.__dbt_internal_microbatch_event_time_start ~ "'") %}
    {% endif %}
    {% if model.config.__dbt_internal_microbatch_event_time_end -%}
    {{ log("incremenal append event end time < DBT_INTERNAL_TARGET." ~ model.config.event_time ~ " < '" ~ model.config.__dbt_internal_microbatch_event_time_end ~ "'") }}
        {% do incremental_predicates.append("DBT_INTERNAL_TARGET." ~ model.config.event_time ~ " < '" ~ model.config.__dbt_internal_microbatch_event_time_end ~ "'") %}
    {% endif %}
    {% do arg_dict.update({'incremental_predicates': incremental_predicates}) %}

    delete DBT_INTERNAL_TARGET from {{ target }} AS DBT_INTERNAL_TARGET
    where (
    {% for predicate in incremental_predicates %}
        {%- if not loop.first %}and {% endif -%} {{ predicate }}
    {% endfor %}
    );

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )
{% endmacro %}
