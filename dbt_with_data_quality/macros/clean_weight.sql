{% macro clean_weight(column_name) %}
toFloat64OrNull(replace({{ column_name }}, ',', '.'))
{% endmacro %}
