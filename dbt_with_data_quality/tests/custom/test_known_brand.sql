-- Falha se a marca não for uma das conhecidas
SELECT *
FROM {{ ref('stg_products') }}
WHERE brand IS NOT NULL
  AND brand NOT IN ('Samsung', 'Apple', 'LG', 'Nike', 'Adidas')
