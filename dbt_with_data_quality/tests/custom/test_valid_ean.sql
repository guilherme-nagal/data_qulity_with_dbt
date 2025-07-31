-- Falha se o EAN não tiver 13 dígitos ou contiver caracteres não numéricos
SELECT *
FROM {{ ref('stg_products') }}
WHERE NOT (length(ean) = 13 AND match(ean, '^[0-9]{13}$'))
