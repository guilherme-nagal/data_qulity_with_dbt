{{ config(
    order_by='(product_id)', 
    engine='MergeTree()', 
    materialized='table',
) }}

SELECT
  -- ID do produto
  product_id,

  -- Nome do produto: remove espaços extras e capitaliza
  CASE
      WHEN name = '' THEN NULL
      ELSE initcap(trim(name))
  END AS name,

  -- Descrição
  description,

  -- Categoria padronizada
  {{ standardize_category("category") }} AS category,

  -- Preço limpo
  {{ clean_price("price") }} AS price,

  -- Quantidade
  IF(quantity IS NULL OR quantity < 0, 0, quantity) AS quantity,

  -- Datas
  created_at,
  updated_at,

  -- Ativo
  COALESCE(is_active, 0) AS is_active,

  -- Marca
  {{ standardize_brand("brand") }} AS brand,

  -- EAN formatado
  {{ format_ean("ean") }} AS ean,

  -- Peso convertido
  {{ clean_weight("weight") }} AS weight

FROM {{ source('raw', 'products') }}
