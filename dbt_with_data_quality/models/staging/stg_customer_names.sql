{{ config(
    order_by='(name_part)', 
    engine='MergeTree()', 
    materialized='table',
) }}

SELECT DISTINCT
    name_part
FROM {{ source('raw', 'customer_names') }}