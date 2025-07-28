{{ config(
    order_by='(customer_id)', 
    engine='MergeTree()', 
    materialized='table'
) }}

WITH
  domains AS (
    SELECT array(
      'gmail.com', 'example.org', 'example.net', 
      'example.com', 'bol.com', 'uol.com.br'
    ) AS lista
  ),

  base_customers AS (
    SELECT
        customer_id,
        name,
        email,
        phone,
        birth,
        created_at,
        updated_at,
        is_active,
        gender,
        cpf,
        address,
        city,
        state,
        zip_code,
        origin
    FROM {{ source('raw', 'customers') }}
  ),

  cleaned_base AS (
    SELECT
        CAST(customer_id AS Int64) AS customer_id,
        
        -- Nome
        {{ clean_name("name") }} AS name,

        -- Email
        replaceAll(email, ',', '.') AS email_raw,

        -- Domínio
        arrayFilter(x -> position(email, x) > 0, d.lista) AS dom_encontrado,

        -- Correção do email
        CASE
            WHEN position(email, '@') = 0 AND length(arrayFilter(x -> position(email, x) > 0, d.lista)) > 0 THEN
                concat(
                    substring(email, 1, position(email, dom_encontrado[1]) - 1),
                    '@',
                    dom_encontrado[1]
                )
            ELSE replaceAll(email, ',', '.')
        END AS email_corrigido,

        -- Email válido?
        (position(email_corrigido, '@') > 0 AND position(email_corrigido, '.') > 0) AS email_valid_flag,

        -- Apenas dígitos no telefone
        replaceRegexpAll(phone, '[^0-9]', '') AS phone_digits,

        -- Data de nascimento
        parseDateTimeBestEffortOrNull(birth) AS birth_date,

        -- Datas
        created_at,
        updated_at,

        -- Ativo
        coalesce(is_active, 0) AS is_active,

        -- Gênero
        CASE
            WHEN lower(gender) IN ('m', 'masculino') THEN 'M'
            WHEN lower(gender) IN ('f', 'feminino') THEN 'F'
            WHEN lower(gender) = 'outro' THEN 'Other'
            ELSE 'Undefined'
        END AS gender,

        -- CPF tratado
        {{ clean_cpf("cpf") }} AS cpf,

        -- Endereço
        trimBoth(replace(address, '\n', ', ')) AS address,

        -- Cidade
        trimBoth(lower(city)) AS city,

        -- Estado
        CASE
            WHEN state = '' THEN NULL 
            ELSE upper(trimBoth(state)) 
        END AS state,

        -- CEP
        NULLIF(replaceRegexpAll(zip_code, '[^0-9]', ''), '00000') AS zip_code,

        -- Origem
        lower(trimBoth(origin)) AS origin
    FROM base_customers, domains AS d
  ),

  final_customers AS (
    SELECT
      *,
      {{ format_brazilian_phone("phone_digits") }} AS phone
    FROM cleaned_base
  ),

  first_last_name AS (
    SELECT
      c.*,
      p1.name_part AS first_name,
      p2.name_part AS last_name
    FROM final_customers c
    JOIN bix.stg_customer_names AS p1 ON startsWith(c.name, lowerUTF8(p1.name_part))
    JOIN bix.stg_customer_names AS p2 ON endsWith(c.name, lowerUTF8(p2.name_part))
  ),

  first_last_name_group AS (
    SELECT
        customer_id,
        any(name) AS name,
        argMax(first_name, length(first_name)) AS first_name,
        argMax(last_name, length(last_name)) AS last_name,
        any(email_raw) AS email_raw,
        any(email_corrigido) AS email_corrigido,
        any(email_valid_flag) AS email_valid_flag,
        any(phone) AS phone,
        any(birth_date) AS birth_date,
        any(created_at) AS created_at,
        any(updated_at) AS updated_at,
        any(is_active) AS is_active,
        any(gender) AS gender,
        any(cpf) AS cpf,
        any(address) AS address,
        any(city) AS city,
        any(state) AS state,
        any(zip_code) AS zip_code,
        any(origin) AS origin
    FROM first_last_name
    GROUP BY customer_id
  )

SELECT
  customer_id,
  first_name,
  last_name,
  email_corrigido AS email,
  phone,
  birth_date,
  created_at,
  updated_at,
  is_active,
  gender,
  cpf,
  address,
  city,
  state,
  zip_code,
  origin
FROM first_last_name_group
