
WITH DuplicateNames AS (
  SELECT
    first_name,
    last_name,
    COUNT(*) AS name_count
  FROM {{ ref('user_employment_data') }} -- Replace with your actual table name
  GROUP BY
    first_name,
    last_name
  HAVING COUNT(*) > 1
)

SELECT t.*
FROM {{ ref('user_employment_data') }} AS t
JOIN DuplicateNames AS d
ON t.first_name = d.first_name AND t.last_name = d.last_name
ORDER BY t.first_name, t.last_name