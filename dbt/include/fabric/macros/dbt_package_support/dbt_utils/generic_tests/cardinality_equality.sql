{% macro fabric__test_cardinality_equality(model, column_name, to, field) %}

with table_a as (
    select
        {{ column_name }} as cardinality_value,
        count(*) as num_rows
    from {{ model }}
    group by {{ column_name }}
),
table_b as (
    select
        {{ field }} as cardinality_value,
        count(*) as num_rows
    from {{ to }}
    group by {{ field }}
),
except_a as (
    select * from table_a
    except
    select * from table_b
),
except_b as (
    select * from table_b
    except
    select * from table_a
)

select * from except_a
union all
select * from except_b

{% endmacro %}
