import pandas as pd
import os
import sys

# URLs of the datasets
def extract_data():
    airports_url = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports-extended.dat'
    airlines_url = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat'
    routes_url = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat'


    # Load datasets into DataFrames
    airports = pd.read_csv(airports_url, header=None)
    airlines = pd.read_csv(airlines_url, header=None)
    routes = pd.read_csv(routes_url, header=None)

    # Assign column names based on the data structure
    airports.columns = [
        'airport_id',           # 1
        'name',                 # 2
        'city',                 # 3
        'country',              # 4
        'iata_code',            # 5
        'icao_code',            # 6
        'latitude',             # 7
        'longitude',            # 8
        'Altitude',            # 9
        'timezone_offset',      # 10 (replaces 'timezone')
        'DST',                 # 11
        'time_zone_identifier', # 12 (replaces 'timezone')
        'airport_type',         # 13
        'source'                # 14
    ]
    airlines.columns = columns = [
        'id',            # Unique identifier for each airline entry
        'name',          # Name of the airline
        'alias',         # Alternative name or alias for the airline
        'iata_code',     # IATA code for the airline
        'icao_code',     # ICAO code for the airline
        'callsign',      # Callsign for the airline
        'country',       # Country where the airline is registered
        'active'         # Indicates if the airline is active (Y for yes, N for no)
    ]
    routes.columns = [
        'airline_code',      # Code of the airline
        'flight_number',     # Flight number associated with the route
        'source_airport',    # IATA code of the source airport
        'source_airport_id', # Unique identifier for the source airport
        'destination_airport', # IATA code of the destination airport
        'destination_airport_id', # Unique identifier for the destination airport
        'codeshare',         # Codeshare indicator (if applicable)
        'stopovers',         # Number of stopovers (0 for direct flights)
        'equipment_type'     # Type of aircraft used for the flight
    ]
  # Get the path to the parent directory
    parent_directory = os.path.dirname(os.path.dirname(__file__))

    # Define the path to the 'rawdata' directory inside the parent directory
    directory = os.path.join(parent_directory, 'Data', 'rawdata')

    # Ensure the directory exists

    airlines.to_csv(os.path.join(directory, 'airlines.csv'), index=False)
    airports.to_csv(os.path.join(directory, 'airports.csv'), index=False)
    routes.to_csv(os.path.join(directory, 'routes.csv'), index=False)
    return airports, airlines, routes

