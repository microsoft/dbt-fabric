{% macro fabric__get_url_host(field) -%}
    (
        select cast(
            case
                when charindex('/', stripped_url) > 0
                    then left(stripped_url, charindex('/', stripped_url) - 1)
                when charindex('?', stripped_url) > 0
                    then left(stripped_url, charindex('?', stripped_url) - 1)
                else stripped_url
            end
            as varchar(8000)
        )
        from (
            select replace(
                replace(
                    replace({{ field }}, 'android-app://', ''),
                    'http://',
                    ''
                ),
                'https://',
                ''
            ) as stripped_url
        ) as url_host
    )
{%- endmacro %}
