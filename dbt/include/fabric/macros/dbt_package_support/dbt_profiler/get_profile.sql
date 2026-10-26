{% macro fabric__get_profile(relation, exclude_measures=[], include_columns=[], exclude_columns=[], where_clause=none, group_by=[]) %}
    {% if include_columns and exclude_columns %}
        {{ exceptions.raise_compiler_error("Provide only one of include_columns and exclude_columns to get_profile.") }}
    {% endif %}
    {% if not execute %}{{ return("") }}{% endif %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set selected = [] %}
    {% for column in columns %}
        {% if (not include_columns or column.name in include_columns) and column.name not in exclude_columns %}
            {% do selected.append(column) %}
        {% endif %}
    {% endfor %}
    {% if not selected %}
        {{ exceptions.raise_compiler_error("No columns selected for profiling " ~ relation) }}
    {% endif %}
    {% set all_measures = [
        "row_count", "not_null_proportion", "distinct_proportion", "distinct_count",
        "is_unique", "min", "max", "avg", "median", "std_dev_population", "std_dev_sample"
    ] %}
    {% set measures = all_measures | reject("in", exclude_measures) | list %}
    {% set names = columns | map(attribute="name") | map("lower") | list %}
    with source_data as (
        select * from {{ relation }}
        {% if where_clause %}where {{ where_clause }}{% endif %}
    ),
    median_data as (
        select source_data.*
        {% for column in selected %}
            {% if "median" in measures and dbt_profiler.is_numeric_dtype(column.dtype | lower) %}
                {% set alias = "__dbt_profiler_median_" ~ loop.index %}
                {% if alias in names %}
                    {{ exceptions.raise_compiler_error("Profiling requires reserved column name " ~ alias ~ " to be absent from " ~ relation) }}
                {% endif %}
                , percentile_cont(0.5) within group (order by {{ column.quoted }})
                    over ({% if group_by %}partition by {{ group_by | join(", ") }}{% endif %})
                    as {{ adapter.quote(alias) }}
            {% endif %}
            {% if dbt_profiler.is_numeric_dtype(column.dtype | lower)
                and ("std_dev_population" in measures or "std_dev_sample" in measures) %}
                {% set alias = "__dbt_profiler_deviation_" ~ loop.index %}
                {% if alias in names %}
                    {{ exceptions.raise_compiler_error("Profiling requires reserved column name " ~ alias ~ " to be absent from " ~ relation) }}
                {% endif %}
                {# Center values before STDEV to avoid cancellation for large, nearby numbers. #}
                , cast({{ column.quoted }} as float) - avg(cast({{ column.quoted }} as float))
                    over ({% if group_by %}partition by {{ group_by | join(", ") }}{% endif %})
                    as {{ adapter.quote(alias) }}
            {% endif %}
        {% endfor %}
        from source_data
    ),
    column_profiles as (
        {% for column in selected %}
            {% set column_index = loop.index %}
            {% set data_type = column.dtype | lower %}
            select
                {% for group in group_by %}{{ group }},{% endfor %}
                cast('{{ column.name | lower | replace("'", "''") }}' as varchar(128)) as column_name,
                cast('{{ data_type | replace("'", "''") }}' as varchar(128)) as data_type,
                {% for measure in measures %}
                    {% if measure == "median" %}
                        {% if dbt_profiler.is_numeric_dtype(data_type) %}
                            max({{ adapter.quote("__dbt_profiler_median_" ~ column_index) }})
                        {% else %}
                            cast(null as {{ dbt.type_float() }})
                        {% endif %}
                    {% else %}
                        {% set measure_column = column.name %}
                        {% if measure in ["std_dev_population", "std_dev_sample"] and dbt_profiler.is_numeric_dtype(data_type) %}
                            {% set measure_column = "__dbt_profiler_deviation_" ~ column_index %}
                        {% endif %}
                        {{ dbt_profiler["measure_" ~ measure](measure_column, data_type) }}
                    {% endif %} as {{ adapter.quote(measure) }},
                {% endfor %}
                cast({{ dbt.current_timestamp() }} as {{ dbt.type_string() }}) as profiled_at,
                count_big(*) as _profile_row_count
            from median_data
            {% if group_by %}group by {{ group_by | join(", ") }}{% endif %}
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    )
    select
        {% for group in group_by %}{{ group }},{% endfor %}
        column_name, data_type,
        {% for measure in measures %}{{ adapter.quote(measure) }},{% endfor %}
        profiled_at
    from column_profiles
{% endmacro %}
