{%- materialization test, adapter='fabric' -%}

  {% set relations = [] %}
  {% set limit = config.get('limit') %}
  {% set fail_calc = config.get('fail_calc') %}
  {% set warn_if = config.get('warn_if') %}
  {% set error_if = config.get('error_if') %}
  {% set number_of_errors = 0 %}

  {% if should_store_failures() %}

    {% set identifier = model['alias'] %}
    {% set store_failures_as = config.get('store_failures_as') %}
    {% if store_failures_as == none %}{% set store_failures_as = 'table' %}{% endif %}
    {% if store_failures_as not in ['table', 'view'] %}
        {{ exceptions.raise_compiler_error(
            "'" ~ store_failures_as ~ "' is not a valid value for `store_failures_as`. "
            "Accepted values are: ['ephemeral', 'table', 'view']"
        ) }}
    {% endif %}

    -- create an error count view because data tests can be CTE's
    {% set error_vw_relation = api.Relation.create(identifier=identifier ~ '__dbt_err_count_vw', schema=schema, database=database, type='view') -%}
    {% do adapter.drop_relation(error_vw_relation) %}
    {% call statement(auto_begin=True) %}
        {{ get_create_sql(error_vw_relation, sql) }}
    {% endcall %}

    -- Get errors for the data test
    {% call statement('find_if_errors_exists', fetch_result=true) %}
        {{ get_use_database_sql(database) }}
        select count(*) as num_of_errors from {{ error_vw_relation }}
        {{ apply_label() }}
    {% endcall %}

    -- Count errors & drop error view relation
    {% set number_of_errors = load_result('find_if_errors_exists')['data'][0][0] %}
    {% do adapter.drop_relation(error_vw_relation) %}

    {% set target_relation = api.Relation.create(
    identifier=identifier, schema=schema, database=database, type=store_failures_as) -%}

    {% set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
    {% if old_relation %}
        {% do adapter.drop_relation(old_relation) %}
    {% endif %}

    {% call statement(auto_begin=True) %}
        {{ get_create_sql(target_relation, sql) }}
    {% endcall %}
    -- Dropping temp view created during table creation to persist store failures
    {% if target_relation.type == 'table' %}
        {% set tmp_vw_relation = target_relation.incorporate(path={"identifier": target_relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}
        {% do adapter.drop_relation(tmp_vw_relation) %}
    {% endif %}

    {% do relations.append(target_relation) %}

    {% set main_sql %}
        select * from {{ target_relation }}
    {% endset %}
    {{ adapter.commit() }}
  {% else %}
    {% set main_sql = sql %}
  {% endif %}

  {% call statement('main', fetch_result=True) -%}
    {{ get_fabric_test_sql(database, schema, main_sql, fail_calc, warn_if, error_if, limit)}}
  {%- endcall %}

  {% if number_of_errors == 0 and should_store_failures() %}
    -- Dropping target relation if no errors found in data test.
    {% set identifier = model['alias'] %}
    {% set target_relation = api.Relation.create(
    identifier=identifier, schema=schema, database=database, type=store_failures_as) -%}
    {% do adapter.drop_relation(target_relation) %}
  {% endif %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
