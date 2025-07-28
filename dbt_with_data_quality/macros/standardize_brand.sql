{% macro standardize_brand(column_name) %}
CASE
  WHEN lower(trim({{ column_name }})) LIKE '%samsung%' THEN 'Samsung'
  WHEN lower(trim({{ column_name }})) LIKE '%apple%' THEN 'Apple'
  WHEN lower(trim({{ column_name }})) LIKE '%lg%' THEN 'LG'
  WHEN lower(trim({{ column_name }})) LIKE '%nike%' THEN 'Nike'
  WHEN lower(trim({{ column_name }})) LIKE '%adidas%' THEN 'Adidas'
  ELSE NULL
END
{% endmacro %}
