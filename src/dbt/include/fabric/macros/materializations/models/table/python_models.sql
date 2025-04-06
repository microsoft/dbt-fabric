{% macro py_write_table(compiled_code, target_relation) %}
{{ compiled_code }}
dbt = dbtObj()
df = model(dbt, spark)

df.write \
    .mode("overwrite") \
    .synapsesql("{{target_relation}}")
{%- endmacro -%}