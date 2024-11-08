import pandas as pd
import psycopg2
import threading
from dotenv import load_dotenv
from ..ETL_scripts.extract import extract_data


import os
# Function to setup database connection for each thread
def setup_database_connection():
    # Define your connection parameters
    
# Load environment variables from .env file
    load_dotenv()

    # Get database connection details from environment variables
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')


    # Connect to the database
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            dbname=DB_NAME,
            password=DB_PASSWORD
            
        )
        print("Database connection successful.")
    except Exception as e:
        print(f"Error: {e}")
    return connection
    


def insert_airports(airports):
    conn = setup_database_connection()
    cursor = conn.cursor()
    
    for index, row in airports.iterrows():
        row = row.where(pd.notnull(row), None)

        cursor.execute("""
    INSERT INTO raw_data.airports (airport_id, name, city, country, iata_code, icao_code, latitude, longitude, altitude, timezone_offset, DST, time_zone_identifier, airport_type, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
     ON CONFLICT (airport_id) DO NOTHING
""", (
    row["airport_id"], row['name'], row['city'], row['country'], row['iata_code'], row['icao_code'],
    row['latitude'], row['longitude'], row['Altitude'], row['timezone_offset'],
    row['DST'], row['time_zone_identifier'], row["airport_type"], row['source']))
    
    conn.commit()
    cursor.close()
    conn.close()

# Insert airlines function
def insert_airlines(airlines):
    conn = setup_database_connection()
    cursor = conn.cursor()
    
    for index, row in airlines.iterrows():
        row = row.where(pd.notnull(row), None)

        cursor.execute("""
                INSERT INTO raw_data.airlines (id,name, alias, iata_code, icao_code, callsign, country, active)
                VALUES (%s,%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (row["id"],row['name'], row['alias'], row['iata_code'], row['icao_code'], row['callsign'],
                row['country'], row['active']))
    
    conn.commit()
    cursor.close()
    conn.close()

# Insert routes function
def insert_routes(routes):
    conn = setup_database_connection()
    cursor = conn.cursor()
    
    for index, row in routes.iterrows():
        row = row.where(pd.notnull(row), None)

        

        cursor.execute("""
                INSERT INTO raw_data.routes (airline_code, flight_number,source_airport, source_airport_id,destination_airport, destination_airport_id, codeshare, stopovers, equipment_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (airline_code, flight_number, source_airport, destination_airport) DO NOTHING
                
            """, (row['airline_code'], row['flight_number'],row['source_airport'], row['source_airport_id'],row['destination_airport'], row['destination_airport_id'],
                row['codeshare'], row['stopovers'], row['equipment_type']))
    
    conn.commit()
    cursor.close()
    conn.close()

# Threaded function to handle concurrent execution
def thread_function(func, *args):
    """Helper function to run insertion functions in separate threads."""
    thread = threading.Thread(target=func, args=args)
    thread.start()
    return thread

# Main function to load data and run ETL
def load_data(airports, airlines, routes):
    """
    Execute the end-to-end ETL pipeline for the OpenFlights data.
    """
    # Create threads for each insertion task
    airport_thread = thread_function(insert_airports, airports)
    airline_thread = thread_function(insert_airlines, airlines)
    route_thread = thread_function(insert_routes, routes)
    
    # Wait for all threads to complete
    airport_thread.join()
    airline_thread.join()
    route_thread.join()

def inset_raw_data_to_db():
    sql_script = """

    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE SCHEMA IF NOT EXISTS raw_data;

    CREATE TABLE IF NOT EXISTS raw_data.airports (
        airport_id INTEGER primary key,
        name TEXT,
        city TEXT,
        country TEXT,
        iata_code TEXT,
        icao_code TEXT,
        latitude FLOAT,
        longitude FLOAT,
        altitude INTEGER,
        timezone_offset TEXT,
        DST TEXT,
        time_zone_identifier TEXT,
        airport_type TEXT,
        source TEXT
    );

    CREATE TABLE IF NOT EXISTS raw_data.airlines (
        id INTEGER primary key,
        name TEXT,
        alias TEXT,
        iata_code TEXT ,  
        icao_code TEXT,
        callsign TEXT,
        country TEXT,
        active TEXT
    );

    CREATE TABLE IF NOT EXISTS raw_data.routes (
        airline_code TEXT,
        flight_number TEXT,
        source_airport TEXT,
        source_airport_id TEXT,
        destination_airport TEXT,
        destination_airport_id TEXT,
        codeshare TEXT,
        stopovers INTEGER,
        equipment_type TEXT,
        PRIMARY KEY (airline_code, flight_number, source_airport, destination_airport)

        
    );

    """

    conn=setup_database_connection()
    conn.autocommit = True 
    cursor = conn.cursor()
    print("connect")
    cursor.execute(sql_script)
    print("Schema, tables created..")
    airports, airlines, routes = extract_data()
    load_data(airports, airlines, routes)
    print("Data inserted successfully")




