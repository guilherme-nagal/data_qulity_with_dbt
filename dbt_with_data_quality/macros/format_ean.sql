{% macro format_ean(column_name) %}
CASE
  WHEN length(trim({{ column_name }})) = 8 THEN lpad(trim({{ column_name }}), 13, '0')
  ELSE trim({{ column_name }})
END
{% endmacro %}
