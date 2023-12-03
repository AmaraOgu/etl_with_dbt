# Overview

The content in this repository is published as blogs on [Medium](https://medium.com/@amarachi.ogu/hands-on-etl-project-with-python-dbt-and-postgresql-384486bb4e32)  

## Scenario
XYZ Ltd, a small-to-medium business, is looking to unify their customer data from multiple systems into a single database. Their customer data is currently spread across various systems and in different formats. E.g. Credit card data is only stored by the financial systems, Employment is only within the HR systems, etc.

## Objective
The ultimate objective is to standardize and create a single, cohesive record representing all of their customer data, which will then be entered into a relational database system. This will allow them to more easily analyze and exploit their data.

## Architecture Diagram
![Workflow Architecture Diagram](https://miro.medium.com/v2/resize:fit:1400/format:webp/1*J3a4NOPLjpndjXmREBRebw.png) 

## File Layout
```
.
├── README.md                               - Provides an overview and instructions for running the project
├── data_extraction_scripts
│   ├── extract_data.py                     - Python script for extract data from local machine
│   └── util.py                             - defines parameters to connect to database
├── datasets
│   ├── user_data.csv                       - customer vehicle data
│   ├── user_data.json                      - customer credit card data
│   ├── user_data.txt                       - additional information about all the data 
│   └── user_data.xml                       - customer employment data
└── dbt_workflow
    ├── README.md
    ├── dbt
    │   └── profiles.yml                    - Configuration file for dbt profiles
    ├── dbt_project.yml                     - Configuration file for dbt project
    └── models
        ├── analytics
        │   └── unified_user_data.sql       - SQL file defining the unified data.
        └── staging
            ├── schema.yml                  - YAML file defining the schema for staging models.
            ├── shared_names.sql            - SQL file defining customers with shared names.
            ├── source.yml                  - YAML file defining the source for staging models.
            ├── user_credit_card_data.sql   - SQL file defining the staging credit card data model.
            ├── user_employment_data.sql    - SQL file defining the staging employment data model.
            └── user_vehicle_data.sql       - SQL file defining the staging vehicle data model.
```

## Tools
Python - Used for data extraction.  
Postgresql Database - Used to store data.  
Data Build Tool (DBT) - Used for data transformation.  

Understanding The Data  
Data Extraction  
Data Transformation  
Loading the unified data  


## Getting Started
You will also need to have the following tools installed on your machine:
Python
psycopg2
dbt-postgres adapter

You will also need to have a running PostgreSQL database that Python and DBT can connect to. 

To extract the data, follow these steps:
Clone the repository to your machine:
```
https://github.com/AmaraOgu/etl_with_dbt.git
```

Navigate to the `data_extraction_scripts/util.py` file and update the `db_params` variable to enable connection to the database.

Navigate to the `dbt_workflow/dbt/profiles.yml` file and update the file to enable DBT connection to your PostgreSQL database.

From the `etl_with_dbt` directory, depending on your Python version, run the command:
```
python3.9 data_extraction_scripts/extract_data.py
```

Change to the dbt_workflow directory

``` 
cd dbt_workflow
```

The run the dbt workload with the command:
```
dbt run - profiles-dir dbt
```

The resulting table is then loaded into the analytics schema in the PostgreSQL database. This will allow XYZ Ltd to more easily analyze and exploit their data.

The content in this repository is published as blogs on [Medium](https://medium.com/@amarachi.ogu/hands-on-etl-project-with-python-dbt-and-postgresql-384486bb4e32)  

