{% macro fabric__get_url_path(field) -%}
    (
        select cast(
            case
                when charindex('/', stripped_url) = 0 then ''
                when charindex('?', stripped_url, charindex('/', stripped_url)) > 0
                    then substring(
                        stripped_url,
                        charindex('/', stripped_url),
                        charindex('?', stripped_url, charindex('/', stripped_url))
                            - charindex('/', stripped_url)
                    )
                else substring(
                    stripped_url,
                    charindex('/', stripped_url),
                    len(stripped_url)
                )
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
        ) as url_path
    )
{%- endmacro %}
