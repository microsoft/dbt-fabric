{%- macro fabric__deduplicate(relation, partition_by, order_by) -%}

    select top 1 with ties
        *
    from {{ relation }}
    order by row_number() over (
        partition by {{ partition_by }}
        order by {{ order_by }}
    )

{%- endmacro -%}
