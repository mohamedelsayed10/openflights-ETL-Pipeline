import pandas as pd
import psycopg2
import threading
from dotenv import load_dotenv
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

    # Connect to the database
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname='openflights',
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Database connection successful.")
    except Exception as e:
        print(f"Error: {e}")
    return connection

# Insert airports function
def insert_airports(airports):
    conn = setup_database_connection()
    cursor = conn.cursor()
    
    for index, row in airports.iterrows():
        # Check if the airport already exists in the database
      
        cursor.execute("""
                INSERT INTO db_SCHEMA1.dim_airports (airport_id,name, city, country, iata_code, icao_code, latitude, longitude, Altitude, timezone_offset, DST, time_zone_identifier, airport_type, geom)
                VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
                on conflict (airport_id) do nothing
            """, (row["airport_id"],row['name'], row['city'], row['country'], row['iata_code'], row['icao_code'],
                  row['latitude'], row['longitude'], row['Altitude'], row['timezone_offset'],
                  row['DST'], row['time_zone_identifier'], row["airport_type"], row['geom']))
    
    conn.commit()
    cursor.close()
    conn.close()

# Insert airlines function
def insert_airlines(airlines):
    conn = setup_database_connection()
    cursor = conn.cursor()
    
    for index, row in airlines.iterrows():
        # Check if the airline already exists in the database

       
        cursor.execute("""
                INSERT INTO db_SCHEMA1.dim_airlines (airline_id,name, alias, iata_code, icao_code, callsign, country, active)
                VALUES (%s,%s, %s, %s, %s, %s, %s, %s)
                on conflict (airline_id) do nothing
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
        # Check if the route already exists in the database

        cursor.execute("""
                INSERT INTO db_SCHEMA1.fact_routes (airline_id, flight_number, source_airport_id, destination_airport_id, codeshare, stopovers, equipment_type, distance_km)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (airline_id, flight_number, source_airport_id, destination_airport_id) do nothing
            """, (row['airline_id'], row['flight_number'], row['source_airport_id'], row['destination_airport_id'],
                  row['codeshare'], row['stopovers'], row['equipment_type'], row['distance_km']))
    
    conn.commit()
    cursor.close()
    conn.close()

# Threaded function to handle concurrent execution
def thread_function(func, completion_event, *args):
    """Helper function to run insertion functions in separate threads and set completion events."""
    def wrapper():
        func(*args)
        completion_event.set()  # Signal completion
    thread = threading.Thread(target=wrapper)
    thread.start()
    return thread

def load_data(airports, airlines, routes):
    """
    Execute the end-to-end ETL pipeline for the OpenFlights data.
    """

    # Create events to signal completion of airport and airline inserts
    airport_done = threading.Event()
    airline_done = threading.Event()

    # Start threads with completion events
    airport_thread = thread_function(insert_airports, airport_done, airports)
    airline_thread = thread_function(insert_airlines, airline_done, airlines)

    # Wait for both threads to signal completion before inserting routes
    airport_done.wait()
    airline_done.wait()

    # Now insert routes after airports and airlines are inserted
    insert_routes(routes)
    airport_thread.join()
    airline_thread.join()

