{% macro standardize_category(column_name) %}
CASE
  WHEN lower(trim({{ column_name }})) IN ('eletrônicos', 'eletronicos') THEN 'Eletrônicos'
  WHEN lower(trim({{ column_name }})) LIKE '%livr%' THEN 'Livros'
  WHEN lower(trim({{ column_name }})) IN ('clothes', 'roupas') THEN 'Roupas'
  ELSE NULL
END
{% endmacro %}
