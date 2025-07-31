-- Falha se houver preços negativos ou nulos
SELECT *
FROM {{ ref('stg_products') }}
WHERE price IS NULL OR price < 0
