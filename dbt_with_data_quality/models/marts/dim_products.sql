{{ config(
    order_by='(product_id)', 
    engine='MergeTree()', 
    materialized='table',
    ) 
}}

SELECT *
FROM {{ ref('stg_products') }}
WHERE name IS NOT NULL
  -- AND price > 0
  -- AND quantity >= 0
  -- AND weight IS NOT NULL
