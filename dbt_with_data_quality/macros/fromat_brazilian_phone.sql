{% macro format_brazilian_phone(column_name) %}
CASE
  WHEN {{ column_name }} = '' OR {{ column_name }} IS NULL THEN NULL
  WHEN startsWith({{ column_name }}, '550') THEN concat('+55', substring({{ column_name }}, 3))
  WHEN startsWith({{ column_name }}, '55') AND length({{ column_name }}) > 11 THEN concat('+', {{ column_name }})
  WHEN length({{ column_name }}) = 11 THEN concat('+55', {{ column_name }})
  WHEN length({{ column_name }}) = 10 THEN concat('+55', {{ column_name }})
  ELSE concat('+55', {{ column_name }})
END
{% endmacro %}
