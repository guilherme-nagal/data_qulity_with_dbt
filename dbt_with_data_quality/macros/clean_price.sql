{% macro clean_price(column_name) %}
abs(toFloat64OrNull(
  replaceRegexpAll(
    replace(trim({{ column_name }}), 'R$', ''),
    '[^0-9.,]', ''  -- remove letras e outros símbolos
  )
))
{% endmacro %}
