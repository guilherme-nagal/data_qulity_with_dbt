{% macro clean_name(column_name) %}
    replaceRegexpAll(
        trimBoth(lowerUTF8(
            multiIf(
                position({{ column_name }}, '.') > 0,
                substring({{ column_name }}, position({{ column_name }}, '.') + 1),
                {{ column_name }}
            )
        )),
        '\\s+', ' '
    )
{% endmacro %}
