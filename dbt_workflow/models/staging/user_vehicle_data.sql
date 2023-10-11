-- models/staging/user_vehicle_data.sql

SELECT * 
FROM {{ source('raw_data', 'user_data_csv') }}
ORDER BY
first_name ASC

