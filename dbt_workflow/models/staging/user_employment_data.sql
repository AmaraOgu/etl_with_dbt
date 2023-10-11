-- models/staging/user_employment_data.sql

WITH xml_unnested AS (
  SELECT
    unnest(xpath('/user/@firstName', user_data)) AS first_name,
    unnest(xpath('/user/@lastName', user_data)) AS last_name,
    unnest(xpath('/user/@age', user_data)) AS age,
    unnest(xpath('/user/@sex', user_data)) AS sex,
    unnest(xpath('/user/@retired', user_data)) AS retired,
    unnest(xpath('/user/@dependants', user_data)) AS dependants,
    unnest(xpath('/user/@marital_status', user_data)) AS marital_status,
    unnest(xpath('/user/@salary', user_data)) AS salary,
    unnest(xpath('/user/@pension', user_data)) AS pension,
    unnest(xpath('/user/@company', user_data)) AS company,
    unnest(xpath('/user/@commute_distance', user_data)) AS commute_distance,
    unnest(xpath('/user/@address_postcode', user_data)) AS address_postcode
  FROM {{ source('raw_data', 'user_data_xml') }} 
),

xml_data AS (
  SELECT
    first_name::text,
    last_name::text,
    COALESCE(NULLIF(age::text, ''), '0')::int AS age,
    -- first_name || ' ' || last_name || ' ' || age AS name_and_age, -- Add full_name column
    sex::text,
    retired::text::boolean,
    COALESCE(NULLIF(dependants::text, ''), '0')::int AS dependants,
    marital_status::text,
    salary::text::numeric,
    pension::text::numeric,
    company::text,
    commute_distance::text::numeric,
    address_postcode::text
  FROM xml_unnested
)

SELECT *
FROM xml_data
ORDER BY
  first_name ASC