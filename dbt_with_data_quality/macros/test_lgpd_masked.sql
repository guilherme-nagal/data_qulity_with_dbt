{% test lgpd_masked(model, column_name, regex_pattern) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
      AND NOT match({{ column_name }}, '{{ regex_pattern }}')
{% endtest %}
