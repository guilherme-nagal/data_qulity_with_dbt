{% macro clean_cpf(column_name) %}
    NULLIF(replaceRegexpAll({{ column_name }}, '[^0-9]', ''), '00000000000')
{% endmacro %}
