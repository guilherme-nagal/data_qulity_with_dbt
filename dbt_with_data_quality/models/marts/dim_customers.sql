{{ config(
    order_by='(customer_id)', 
    engine='MergeTree()', 
    materialized='table',
    ) 
}}

SELECT
  customer_id,
  first_name,
  last_name,
  email,
  {{ mask_email("email") }} AS email_masked,
  phone,
  {{ mask_phone("phone") }} AS phone_masked,
  birth_date,
  created_at,
  updated_at,
  is_active,
  gender,
  cpf,
  {{ mask_cpf("cpf") }} AS cpf_masked,
  address,
  city,
  state,
  zip_code,
  origin
FROM {{ ref('stg_customers') }}
WHERE first_name IS NOT NULL

-- SELECT *
-- FROM {{ ref('stg_customers') }}
-- WHERE first_name IS NOT NULL
  -- AND email LIKE '%@%'
  -- AND cpf != '00000000000'
  -- AND gender != 'Indefinido'
