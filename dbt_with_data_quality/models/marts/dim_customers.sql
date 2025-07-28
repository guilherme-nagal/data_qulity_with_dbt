{{ config(
    order_by='(customer_id)', 
    engine='MergeTree()', 
    materialized='table',
    ) 
}}

SELECT *
FROM {{ ref('stg_customers') }}
WHERE first_name IS NOT NULL
  -- AND email LIKE '%@%'
  -- AND cpf != '00000000000'
  -- AND gender != 'Indefinido'
