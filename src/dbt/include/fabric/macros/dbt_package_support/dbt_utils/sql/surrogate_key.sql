{#
    Generates a surrogate key hash like dbt_utils.surrogate_key(), but
    provides additional, T-SQL specific parameters and config.

    Example usage:
        ```sql
        select
            {{ tsql_utils.surrogate_key(["id"]) }} as test_key
        from src_test
        ```

    Args:
        field_list (list): A list of columns or values that should be used to 
                           generate the surrogate key.

        col_type (str): The column type field values will be casted to before
                        hashing. Useful for when the underlying columns are
                        nvarchar, for example.

        use_binary_hash (bool): By default the hash is converted to a varchar
                                string that uses 32 bytes of data. Setting
                                this parameter to True will keep the key as
                                varbinary that only uses 16 bytes of data.
                                
                                This will reduce space in the database and can
                                potentially increase join performance, but the
                                column has to be converted into varchar before
                                it can be used in Power BI for relationships.

    Returns:
        str: SQL code that generates a hashed surrogate key.

    DBT Project Variables:
        You can also adjust default settings through variables in your 
        dbt_project.yml:

        ```yml
        vars:
          dbt_utils_dispatch_list: ['tsql_utils']
          tsql_utils_surrogate_key_col_type: 'nvarchar(1234)'
          tsql_utils_surrogate_key_use_binary_hash: True
        ```
#}


{%- macro surrogate_key(field_list, col_type=None, use_binary_hash=None) -%}

    {%- if col_type == None -%}
        {%- set col_type = var(
            "tsql_utils_surrogate_key_col_type",
            "varchar(8000)"
        ) -%}
    {%- endif -%}

    {%- if use_binary_hash == None -%}
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
    Converts a value from a binary surrogate key hash into varchar.

    This is useful if you are using `use_binary_hash=True` for your surrogate keys. Binary columns cannot be used for relationships in Power BI.
    
    This macro allows you to convert them to varchar inside your report views
    before importing them into Power BI to allow relationships on your
    surrogate key columns.

    Args:
        col (str): The column or value that should be converted from binary 
                   hash to varchar hash.

    Returns:
        str: SQL code that converts a varbinary has to varchar.

#}
{%- macro cast_hash_to_str(col) -%}
  convert(varchar(32), {{ col }}, 2)
{%- endmacro -%}
