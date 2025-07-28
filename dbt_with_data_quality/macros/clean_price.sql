{% macro clean_price(column_name) %}
abs(toFloat64OrNull(
  replaceRegexpAll(
    replace(trim({{ column_name }}), 'R$', ''),
    ',', '.'
  )
))
{% endmacro %}
