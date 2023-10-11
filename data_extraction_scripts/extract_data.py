import csv
import json
import psycopg2
import xml.etree.ElementTree as ET
from util import db_params, SCHEMA

class DataIngestion:
    def __init__(self):
        # Initialize the database connection
        self.conn = psycopg2.connect(**db_params)
        self.cur = self.conn.cursor()

    def create_schema(self):
        # Create the schema if it doesn't exist
        self.cur.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}')
        self.conn.commit()

    def ingest_csv_data(self, csv_file, table_name):
        # Create the table to store CSV data
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{table_name}  (
            first_name VARCHAR(255),
            second_name VARCHAR(255),
            age INTEGER,
            sex VARCHAR(10),
            vehicle_make VARCHAR(255),
            vehicle_model VARCHAR(255),
            vehicle_year INTEGER,
            vehicle_type VARCHAR(255),
            CONSTRAINT unique_user_data UNIQUE (first_name, second_name, age)
        );
        '''
        self.cur.execute(create_table_query)
        self.conn.commit()

        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                # Insert data from CSV into the table (skip duplicates)
                insert_query = f'''
                INSERT INTO {SCHEMA}.{table_name} (first_name, second_name, age, sex, vehicle_make, vehicle_model, vehicle_year, vehicle_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (first_name, second_name, age) DO NOTHING;
                '''
                self.cur.execute(insert_query, row)
                self.conn.commit()

    def ingest_json_data(self, json_file, table_name):
        # Create the table to store JSON data
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{table_name} (
            user_data JSONB
        );
        """
        self.cur.execute(create_table_query)
        self.conn.commit()

        with open(json_file, 'r') as file:
            json_data = json.load(file)
            for record in json_data:
                # Check if a record with the same JSON data already exists
                check_query = f"SELECT 1 FROM {SCHEMA}.{table_name} WHERE user_data = %s LIMIT 1;"
                self.cur.execute(check_query, (json.dumps(record),))
                existing_record = self.cur.fetchone()

                if not existing_record:
                    # Insert data if it doesn't already exist (avoid duplicates)
                    insert_query = f"""
                    INSERT INTO {SCHEMA}.{table_name} (user_data)
                    VALUES (%s);
                    """
                    self.cur.execute(insert_query, (json.dumps(record),))
                    self.conn.commit()

    def ingest_text_data(self, text_file, table_name):
        # Create the table to store text values
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{table_name} (
            text_value TEXT
        );
        """
        self.cur.execute(create_table_query)
        self.conn.commit()

        with open(text_file, 'r') as file:
            for line in file:

                # Check if a record with the same text data already exists
                self.cur.execute(f"SELECT text_value FROM {SCHEMA}.{table_name};")
                existing_record = self.cur.fetchone()
                
                if not existing_record:
                    # Insert data if it doesn't already exist (avoid duplicates)
                    insert_query = f"""
                    INSERT INTO {SCHEMA}.{table_name} (text_value)
                    VALUES (%s);
                    """
                    self.cur.execute(insert_query, (line,))
                    self.conn.commit()

    def ingest_xml_data(self, xml_file, table_name):
        # Create the table to store XML data
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{table_name} (
            user_data XML
        );
        """
        self.cur.execute(create_table_query)
        self.conn.commit()

        tree = ET.parse(xml_file)
        root = tree.getroot()
        for user_element in root:
            user_data = ET.tostring(user_element, encoding='unicode')

            # Check if a record with the same xml data already exists
            self.cur.execute(f"SELECT user_data FROM {SCHEMA}.{table_name};")
            existing_record = self.cur.fetchone()

            if not existing_record:
                # Insert data if it doesn't already exist (avoid duplicates)
                insert_query = f"""
                INSERT INTO {SCHEMA}.{table_name} (user_data)
                VALUES (%s);
                """
                self.cur.execute(insert_query, (user_data,))
                self.conn.commit()

    def close_connection(self):
        # Close the database connection and print a success message
        self.conn.close()
        print(f"Data ingested successfully.")

if __name__ == '__main__':
    data_ingestion = DataIngestion()

    data_ingestion.create_schema()

    # Ingest data from CSV file
    csv_file = 'datasets/user_data.csv'
    data_ingestion.ingest_csv_data(csv_file, 'user_data_csv')

    # Ingest data from JSON file
    json_file = 'datasets/user_data.json'
    data_ingestion.ingest_json_data(json_file, 'user_data_json')

    # Ingest data from text file
    text_file = 'datasets/user_data.txt'
    data_ingestion.ingest_text_data(text_file, 'user_data_txt')

    # Ingest data from XML file
    xml_file = 'datasets/user_data.xml'
    data_ingestion.ingest_xml_data(xml_file, 'user_data_xml')

    # Close the database connection
    data_ingestion.close_connection()
