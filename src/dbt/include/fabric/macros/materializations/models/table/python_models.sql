{%- macro fabric__resolve_model_name(input_model_name) -%}
    {{  input_model_name | string | replace('"', '') | replace("'", '') | replace('[', '') | replace(']', '') }}
{%- endmacro -%}

{% macro py_write_table(compiled_code, target_relation) %}
{{ compiled_code }}

import com.microsoft.spark.fabric

dbt = dbtObj(spark.read.synapsesql)
df = model(dbt, spark)

df.write.mode("overwrite").synapsesql("{{ target_relation.database }}.{{ target_relation.schema }}.{{ target_relation.identifier }}")
{%- endmacro -%}