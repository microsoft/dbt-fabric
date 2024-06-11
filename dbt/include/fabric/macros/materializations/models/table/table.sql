{% materialization table, adapter='fabric' %}

  -- Load target relation
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set existing_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) -%}

  {#-- Drop the relation if it was a view to "convert" it in a table. This may lead to
    -- downtime, but it should be a relatively infrequent occurrence  #}
  {% if existing_relation is not none and not old_relation.is_table %}
    {{ log("Dropping relation " ~ existing_relation ~ " because it is of type " ~ existing_relation.type) }}
    {% do adapter.drop_relation(existing_relation) %}
  {% endif %}

  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}

  -- cleanup
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}
  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- Add constraints including FK relation.
  {{ build_model_constraints(target_relation) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
