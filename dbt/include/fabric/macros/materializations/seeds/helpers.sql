{% macro fabric__get_binding_char() %}
  {{ return('?') }}
{% endmacro %}

{% macro fabric__get_batch_size() %}
  {{ return(400) }}
{% endmacro %}

{% macro fabric__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {# reset_csv_table mutates the target before any statement() call can open the transaction. #}
    {% call statement('begin_seed_reset', auto_begin=True) %}
        select 1
    {% endcall %}

    {% if full_refresh %}
        {{ adapter.drop_relation(old_relation) }}
        {% set sql = create_csv_table(model, agate_table) %}
    {% else %}
        {{ adapter.truncate_relation(old_relation) }}
        {% set sql = "truncate table " ~ old_relation.render() %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}

{% macro calc_batch_size(num_columns) %}
    {#
        SQL Server allows for a max of 2100 parameters in a single statement.
        Check if the max_batch_size fits with the number of columns, otherwise
        reduce the batch size so it fits.
    #}
    {% set max_batch_size = get_batch_size() %}
    {% set calculated_batch = (2100 / num_columns)-1|int %}
    {% set calculated_batch = ((2100 / num_columns)-1)|int %}
    {% set batch_size = [max_batch_size, calculated_batch] | min %}

    {{ return(batch_size) }}
{%  endmacro %}

{% macro fabric__load_csv_rows(model, agate_table) %}
  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set batch_size = calc_batch_size(agate_table.column_names|length) %}
  {% set bindings = [] %}
  {% set statements = [] %}

  {{ log("Inserting batches of " ~ batch_size ~ " records") }}

  {% for chunk in agate_table.rows | batch(batch_size) %}
      {% set bindings = [] %}

      {% for row in chunk %}
          {% do bindings.extend(row) %}
      {% endfor %}

      {% set sql %}
          insert into {{ this.render() }} ({{ cols_sql }}) values
          {% for row in chunk -%}
              ({%- for column in agate_table.column_names -%}
                  {{ get_binding_char() }}
                  {%- if not loop.last%},{%- endif %}
              {%- endfor -%})
              {%- if not loop.last%},{%- endif %}
          {%- endfor %}


      {% endset %}

      {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

      {% if loop.index0 == 0 %}
          {% do statements.append(sql) %}
      {% endif %}
  {% endfor %}

  {# Return SQL so we can render it out into the compiled files #}
  {{ return(statements[0]) }}
{% endmacro %}
