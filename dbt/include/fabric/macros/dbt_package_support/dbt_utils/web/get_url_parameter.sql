{% macro fabric__get_url_parameter(field, url_parameter) -%}
    nullif(
        case
            when charindex('{{ url_parameter }}=', {{ field }}) = 0 then ''
            when charindex(
                '&',
                {{ field }} + '&',
                charindex('{{ url_parameter }}=', {{ field }})
            ) > 0
                then substring(
                    {{ field }},
                    charindex('{{ url_parameter }}=', {{ field }})
                        + len('{{ url_parameter }}='),
                    charindex(
                        '&',
                        {{ field }} + '&',
                        charindex('{{ url_parameter }}=', {{ field }})
                    )
                        - charindex('{{ url_parameter }}=', {{ field }})
                        - len('{{ url_parameter }}=')
                )
            else ''
        end,
        ''
    )
{%- endmacro %}
