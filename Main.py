# Import necessary libraries

import csv

import logging

import requests

import sqlite3

from io import StringIO

# Configure the logging

logging.basicConfig(filename='cities.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# Function to download data from provided URL

def download_data(url):

    try:

        response = requests.get(url)

        response.raise_for_status()

    except requests.exceptions.RequestException as err:

        logging.error(f"Error occurred during request: {err}")

        raise SystemExit(err)

    data = csv.reader(StringIO(response.text), delimiter=';')

    logging.info(f"Data download completed. Response size: {len(response.text)} characters.")

    return data

# Function to delete the existing cities table in the database, if it exists

def recreate_database(conn):

    try:

        c = conn.cursor()

        c.execute('DROP TABLE IF EXISTS cities')

        conn.commit()

        logging.info('Table dropped.')

    except sqlite3.Error as err:

        logging.error(f"Database error occurred during DROP TABLE: {err}")

        raise SystemExit(err)

# Function to create a new table for cities in the database. 

def create_database(conn):

    try:

        c = conn.cursor()

        c.execute('''

            CREATE TABLE cities (

                geoname_id INTEGER,

                name TEXT,

                ascii_name TEXT,

                alternate_names TEXT,

                feature_class TEXT,

                feature_code TEXT,

                country_code TEXT,

                cou_name_en TEXT,

                country_code_2 TEXT,

                admin1_code TEXT,

                admin2_code TEXT,

                admin3_code TEXT,

                admin4_code TEXT,

                population INTEGER,

                elevation INTEGER,

                dem INTEGER,

                timezone TEXT,

                modification_date DATE,

                label_en TEXT,

                coordinates TEXT

            )

        ''')

        conn.commit()

    except sqlite3.Error as err:

        logging.error(f"Database error occurred during CREATE TABLE: {err}")

        raise SystemExit(err)

    logging.info('Database and table created successfully.')

# Function to insert downloaded data into the database table

def insert_data(conn, reader):

    try:

        c = conn.cursor()

        next(reader)

        rows = list(reader)

        c.executemany('''

            INSERT INTO cities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

        ''', rows)

        conn.commit()

        logging.info(f"Data insertion completed. Rows inserted: {len(rows)}.")

    except sqlite3.Error as err:

        logging.error(f"Database error occurred during INSERT: {err}")

        raise SystemExit(err)

# Function to query the database for the required data

def query_data(conn):

    try:

        c = conn.cursor()

        c.execute('''

            SELECT DISTINCT country_code, cou_name_en

            FROM cities

            WHERE country_code NOT IN (

                SELECT DISTINCT country_code

                FROM cities

                WHERE population >= 10000000

            )

            ORDER BY cou_name_en

        ''')

        results = c.fetchall()

        logging.info(f"Select query executed successfully. Rows returned: {len(results)}.")

        return results

    except sqlite3.Error as err:

        logging.error(f"Database error occurred during SELECT: {err}")

        raise SystemExit(err)

# Function to save the results to a file

def save_results(results, filename):

    with open(filename, 'w', newline='') as f:

        writer = csv.writer(f, delimiter='\t')

        writer.writerow(['Country Code', 'Country Name'])

        writer.writerows(results)

    logging.info(f"Query results saved to '{filename}'. Rows written: {len(results)}.")

def main():

    logging.info(f"Started program")

    url = "https://public.opendatasoft.com/api/records/1.0/download/?dataset=geonames-all-cities-with-a-population-1000"

    reader = download_data(url)

    conn = sqlite3.connect('cities_database.db')

    recreate_database(conn)

    create_database(conn)

    insert_data(conn, reader)

    results = query_data(conn)

    conn.close()

    save_results(results, 'countries_without_megapolis.tsv')

if __name__ == "__main__":

    main()
