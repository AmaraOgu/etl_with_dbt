-- {{ config(schema='analytics') }}

WITH vehicle_data AS ( --csv
  SELECT
    *,
    first_name || ' ' || second_name || ' ' || age AS name_and_age -- Add full_name column
  FROM {{ ref('user_vehicle_data') }}
),

credit_card_data AS ( --json
  SELECT
    *,
    first_name || ' ' || last_name || ' ' || age AS name_and_age -- Add full_name column
  FROM {{ ref('user_credit_card_data') }}
),

employment_data AS ( --xml
  SELECT
    *,
    first_name || ' ' || last_name || ' ' || age AS name_and_age -- Add full_name column
  FROM {{ ref('user_employment_data') }}
),

joined_datasets AS (
SELECT
  v.first_name,
  c.last_name,
  v.age,
  e.name_and_age,
  v.sex,
  v.vehicle_make,
  v.vehicle_model,
  v.vehicle_year,
  v.vehicle_type,
  c.iban,
  c.credit_card_number,
  c.credit_card_security_code,
  c.credit_card_start_date,
  c.credit_card_end_date,
  c.address_main,
  c.address_city,
  c.address_postcode,
  c.debt_amount,
  c.debt_time_period_years,
  e.retired,
  e.dependants,
  e.marital_status,
  e.salary,
  e.pension,
  e.company,
  e.commute_distance
FROM vehicle_data v
LEFT JOIN credit_card_data c ON v.name_and_age = c.name_and_age
LEFT JOIN employment_data e ON v.name_and_age = e.name_and_age OR c.name_and_age = e.name_and_age
),

-- Make updates on the table based on the text file
-- 1. update Shane Chambers' credit card security code to '935'.
-- 2. add £2100 to Joshua Lane's salary.
-- 3. update Suzanne Wright's age to 37.
-- 4. modify Kyle Dunn's pension by adding another 0.15% on top of the existing £22358.

updates_based_on_text_file AS (
select
  first_name,
  last_name,
  case
    when first_name = 'Suzanne' AND last_name = 'Wright' THEN 37
    else age
  end as age,
  sex,
  iban,
  credit_card_number,
  case
    when first_name = 'Shane' AND last_name = 'Chambers' THEN '935'
    else credit_card_security_code
  end as credit_card_security_code,
  credit_card_start_date,
  credit_card_end_date,
  address_main,
  address_city,
  address_postcode,
  debt_amount,
  debt_time_period_years,
  retired,
  dependants,
  marital_status,
  case
    when first_name = 'Joshua' AND last_name = 'Lane' THEN  salary + 2100
    else salary
  end as salary,

  case
    when first_name = 'Kyle' AND last_name = 'Dunn' THEN pension + (pension * 0.0015)
    else pension
  end as pension,

  company,
  commute_distance,
  vehicle_make,
  vehicle_model,
  vehicle_year,
  vehicle_type

from
  joined_datasets
  
)

select *
FROM updates_based_on_text_file
ORDER BY
  first_name ASC