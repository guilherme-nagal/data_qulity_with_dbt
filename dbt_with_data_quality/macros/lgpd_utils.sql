{% macro mask_cpf(column_name) %}
    regexp_replace({{ column_name }}, '(\\d{3})\\d{3}(\\d{3})(\\d{2})', '\\1***\\2**')
{% endmacro %}


{% macro mask_email(column_name) %}
    regexp_replace({{ column_name }}, '(^.).*(@.*$)', '\\1***\\2')
{% endmacro %}


{% macro mask_phone(column_name) %}
    regexp_replace({{ column_name }}, '\\d{4}$', '****')
{% endmacro %}

