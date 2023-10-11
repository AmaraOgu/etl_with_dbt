-- models/staging/user_credit_card_data.sql

WITH json_data AS (
SELECT
  user_data->>'firstName' AS first_name,
  user_data->>'lastName' AS last_name,
  (user_data->>'age')::int AS age,
  user_data->>'iban' AS iban,
  user_data->>'credit_card_number' AS credit_card_number,
  user_data->>'credit_card_security_code' AS credit_card_security_code,
  user_data->>'credit_card_start_date' AS credit_card_start_date,
  user_data->>'credit_card_end_date' AS credit_card_end_date,
  user_data->>'address_main' AS address_main,
  user_data->>'address_city' AS address_city,
  user_data->>'address_postcode' AS address_postcode,
  CASE
        WHEN user_data->>'debt' ~ '^{.*}$' THEN (user_data->'debt'->>'amount')::text
        ELSE user_data->>'debt'
    END AS debt_amount,
    CASE
        WHEN user_data->>'debt' ~ '^{.*}$' THEN (user_data->'debt'->>'time_period_years')::text
        ELSE NULL
    END AS debt_time_period_years
FROM {{ source('raw_data', 'user_data_json') }} 

)

SELECT 
    first_name,
    last_name,
    age,
    -- first_name || ' ' || last_name || ' ' || age AS name_and_age, -- Add full_name column
    iban,
    credit_card_number,
    credit_card_security_code,
    credit_card_start_date,
    credit_card_end_date,
    address_main,
    address_city,
    address_postcode,
    COALESCE(NULLIF(debt_amount::text, ''), '0')::numeric AS debt_amount,
    COALESCE(NULLIF(debt_time_period_years::text, ''), '0')::numeric AS debt_time_period_years

FROM json_data

ORDER BY
  first_name ASC