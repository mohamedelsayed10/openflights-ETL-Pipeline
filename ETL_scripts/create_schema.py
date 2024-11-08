import psycopg2
from dotenv import load_dotenv
import os
from psycopg2 import sql

def setup_database_connection(data_base_name):
    # Define your connection parameters
    
# Load environment variables from .env file
    load_dotenv()

    # Get database connection details from environment variables
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME=os.getenv('DB_name')

    # Connect to the database
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=data_base_name,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Database connection successful.")
    except Exception as e:
        print(f"Error: {e}")
    return connection
def create():
    sql_script = """
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE SCHEMA IF NOT EXISTS db_SCHEMA1;

    CREATE TABLE IF NOT EXISTS db_SCHEMA1.dim_airports (
        airport_id Integer PRIMARY KEY,
        name TEXT,
        city TEXT,
        country TEXT,
        iata_code TEXT,
        icao_code TEXT,
        latitude FLOAT,
        longitude FLOAT,
        altitude FLOAT,
        timezone_offset TEXT,
        DST TEXT,
        time_zone_identifier TEXT,
        airport_type TEXT,
        geom GEOMETRY(POINT, 4326)
    );

    CREATE TABLE IF NOT EXISTS db_SCHEMA1.dim_airlines (
        airline_id BIGINT PRIMARY KEY,
        name TEXT,
        alias TEXT,
        iata_code TEXT ,  
        icao_code TEXT,
        callsign TEXT,
        country TEXT,
        active BOOLEAN
    );

    CREATE TABLE IF NOT EXISTS db_SCHEMA1.fact_routes (
        airline_id BIGINT references db_SCHEMA1.dim_airlines(airline_id),
        flight_number INTEGER,
        source_airport_id INTEGER REFERENCES db_SCHEMA1.dim_airports(airport_id),
        destination_airport_id INTEGER REFERENCES db_SCHEMA1.dim_airports(airport_id),
        codeshare BOOLEAN,
        stopovers INTEGER,
        equipment_type TEXT,
        distance_km FLOAT,
        PRIMARY KEY (airline_id, flight_number, source_airport_id, destination_airport_id)

    );

    CREATE INDEX IF NOT EXISTS idx_airport_geom ON db_SCHEMA1.dim_airports USING GIST (geom);
    """
    conn=setup_database_connection("postgres")
    conn.autocommit = True 
    cursor = conn.cursor()
    database_name = "openflights"
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (database_name,))
    exists = cursor.fetchone()

    if exists:
        print(f"Database '{database_name}' already exists.")
    else:
        # Create the new database if it doesn't exist
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(database_name)))
        print(f"Database '{database_name}' created successfully.")
    conn=setup_database_connection(database_name)
    conn.autocommit = True 
    cursor = conn.cursor()
    cursor.execute(sql_script)
    print("Schema, tables,created successfully")