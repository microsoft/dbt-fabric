{#
generates A surrogate key HASH LIKE dbt_utils.surrogate_key(),
but provides additional,
t - SQL specific PARAMETERS
AND config.example usage: ```sql select {{ surrogate_key(["id"]) }} as test_key from src_test ``` args: field_list (list): A list OF COLUMNS
OR
VALUES
    that should be used TO generate THE surrogate key.col_type (str): THE column TYPE field
VALUES
    will be casted TO before hashing.useful for
    WHEN THE underlying COLUMNS are nvarchar,
    for example.use_binary_hash (bool): BY DEFAULT THE HASH IS converted TO A VARCHAR STRING that uses 32 bytes OF DATA.setting this PARAMETER TO TRUE will keep THE key AS varbinary that ONLY uses 16 bytes OF DATA.this will reduce SPACE IN THE database
    AND can potentially increase
    JOIN performance,
    but THE column has TO be converted INTO VARCHAR before it can be used IN power bi for relationships.returns: str: SQL code that generates A hashed surrogate key.dbt project variables: you can also adjust DEFAULT settings through variables IN your dbt_project.yml: ```yml vars: dbt_utils_dispatch_list: ['tsql_utils'] tsql_utils_surrogate_key_col_type: 'nvarchar(1234)' tsql_utils_surrogate_key_use_binary_hash: True ``` #}
    {%- macro surrogate_key(
            field_list,
            col_type = none,
            use_binary_hash = none
        ) -%}
        {%- if col_type == none -%}
            {%- set col_type = var(
                "tsql_utils_surrogate_key_col_type",
                "varchar(8000)"
            ) -%}
        {%- endif -%}

        {%- if use_binary_hash == none -%}
            {%- set use_binary_hash = var(
                "tsql_utils_surrogate_key_use_binary_hash",
                False
            ) -%}
        {%- endif -%}

        {%- if field_list is string -%}
            {%- set field_list = [field_list] -%}
        {%- endif -%}

        {%- set fields = [] -%}
        {%- for field in field_list -%}
            {%- set _ = fields.append(
                "coalesce(cast(" ~ field ~ " as " ~ col_type ~ "), '')"
            ) -%}
            {%- if not loop.last %}
                {%- set _ = fields.append("'-'") -%}
            {%- endif -%}
        {%- endfor -%}

        {%- if use_binary_hash == True -%}
            {%- set key = "hashbytes('md5', " ~ dbt.concat(fields) ~ ")" -%}
        {%- else -%}
            {%- set key = dbt.hash(dbt.concat(fields)) -%}
        {%- endif -%}

        {{ key }}
{%- endmacro -%}

{#
converts A VALUE
FROM
    A BINARY surrogate key HASH INTO VARCHAR.this IS useful if you are USING `use_binary_hash=True` for your surrogate keys.BINARY COLUMNS cannot be used for relationships IN power bi.this macro allows you TO CONVERT them TO VARCHAR inside your report VIEWS before importing them INTO power bi TO allow relationships
    ON your surrogate key COLUMNS.args: col (str): THE column
    OR VALUE that should be converted
FROM
    BINARY HASH TO VARCHAR HASH.returns: str: SQL code that converts A varbinary has TO VARCHAR.#}
    {%- macro cast_hash_to_str(col) -%}
        CONVERT(VARCHAR(32), {{ col }}, 2)
{%- endmacro -%}
