{{ config(
    order_by='(product_id)', 
    engine='MergeTree()', 
    materialized='table',
    ) 
}}

-- models/my_first_model.sql
SELECT
    product_id,
    'dbt + ClickHouse is working!' AS message
FROM {{source("bix", "products")}}
