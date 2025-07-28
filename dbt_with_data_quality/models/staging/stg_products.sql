{{ config(
    order_by='(product_id)', 
    engine='MergeTree()', 
    materialized='table',
) }}


SELECT
  -- ID do produto
  product_id,

  -- Nome do produto: remove espaços extras e capitaliza
  initcap(trim(name)) AS name,

  -- Descrição: mantém como está (já é Nullable)
  description,

  -- Categoria: padronização
  CASE
    WHEN lower(trim(category)) IN ('eletrônicos', 'eletronicos') THEN 'Eletrônicos'
    WHEN lower(trim(category)) LIKE '%livr%' THEN 'Livros'
    WHEN lower(trim(category)) IN ('clothes', 'roupas') THEN 'Roupas'
    ELSE NULL
  END AS category,

  -- Preço: remove 'R$', troca vírgula por ponto e converte para float
  abs(toFloat64OrNull(
    replaceRegexpAll(
      replace(trim(price), 'R$', ''),
      ',', '.'
    )
  )) AS price,

  -- Quantidade: trata nulos como 0 e valores negativos como 0
  IF(quantity IS NULL OR quantity < 0, 0, quantity) AS quantity,

  -- Datas
  created_at,
  updated_at,

  -- Ativo: trata nulo como 0
  COALESCE(is_active, 0) AS is_active,

  -- Marca padronizada
  CASE
    WHEN lower(trim(brand)) LIKE '%samsung%' THEN 'Samsung'
    WHEN lower(trim(brand)) LIKE '%apple%' THEN 'Apple'
    WHEN lower(trim(brand)) LIKE '%lg%' THEN 'LG'
    WHEN lower(trim(brand)) LIKE '%nike%' THEN 'Nike'
    WHEN lower(trim(brand)) LIKE '%adidas%' THEN 'Adidas'
    ELSE NULL
  END AS brand,

  -- EAN: completa com zeros à esquerda se tiver 8 dígitos, senão mantém
  CASE
    WHEN length(trim(ean)) = 8 THEN lpad(trim(ean), 13, '0')
    ELSE trim(ean)
  END AS ean,

  -- Peso: troca vírgula por ponto e converte para float
  toFloat64OrNull(replace(weight, ',', '.')) AS weight

FROM {{ source('raw', 'products') }}