{#- Mirrors default__compare_and_classify_query_results with two T-SQL changes:
    the `true`/`false` side-membership flags become 1/0 integers (T-SQL has no
    boolean literal, and the companion overrides on `_classify_audit_row_status`
    and `_count_num_rows_in_status` read them back), and the final ORDER BY is
    dropped because T-SQL rejects it in a view, CTE or derived table unless TOP,
    OFFSET or FOR XML is present. A materialized table or view has no inherent
    row order anyway, so sort in the query that reads the result instead. -#}
{% macro fabric__compare_and_classify_query_results(a_query, b_query, primary_key_columns, columns, event_time, sample_limit) %}

    {% set columns = audit_helper._ensure_all_pks_are_in_column_set(primary_key_columns, columns) %}

    {% if event_time %}
        {% set event_time_props = audit_helper._get_comparison_bounds(a_query, b_query, event_time) %}
    {% endif %}

    with

    {{ audit_helper._generate_set_results(a_query, b_query, primary_key_columns, columns, event_time_props) }}

    ,

    all_records as (

        select
            *,
            1 as dbt_audit_in_a,
            1 as dbt_audit_in_b
        from a_intersect_b

        union all

        select
            *,
            1 as dbt_audit_in_a,
            0 as dbt_audit_in_b
        from a_except_b

        union all

        select
            *,
            0 as dbt_audit_in_a,
            1 as dbt_audit_in_b
        from b_except_a

    ),

    classified as (
        select
            *,
            {{ audit_helper._classify_audit_row_status() }} as dbt_audit_row_status
        from all_records
    ),

    final as (
        select
            *,
            {{ audit_helper._count_num_rows_in_status() }} as dbt_audit_num_rows_in_status,
            {#- dense_rank keeps both sides of a modified row in the sample; for every other status it matches row_number #}
            dense_rank() over (partition by dbt_audit_row_status order by dbt_audit_surrogate_key, dbt_audit_pk_row_num) as dbt_audit_sample_number
        from classified
    )

    select * from final
    {% if sample_limit %}
        where dbt_audit_sample_number <= {{ sample_limit }}
    {% endif %}

{% endmacro %}
