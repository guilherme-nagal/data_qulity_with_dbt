{% test contains_first_and_last_name(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} not like '% %'
   or length(trim({{ column_name }})) = 0

{% endtest %}