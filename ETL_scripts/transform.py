import pandas as pd
import numpy as np
from geopy.distance import great_circle





def transform_airports(airports):
    airports.replace('\\N', np.nan, inplace=True)
    dst_mapping = {
        'U': 'DST not observed',
        'A': 'US/Canada',
        'E': 'Europe',
        'N': 'None',
        'S': 'South America',
        'O': 'Australia',
        'Z': 'New Zealand',
        np.nan:"DST not observed"

    }
    airports['DST'] = airports['DST'].map(dst_mapping)
    airports['airport_id'] = airports['airport_id'].astype(str)
    airports['Altitude'] = airports['Altitude'].astype(int)
    airports['latitude'] = airports['latitude'].astype(float)
    airports['longitude'] = airports['longitude'].astype(float)
    airports['DST'] = airports['DST'].astype('category')
    airports['country'] = airports['country'].str.strip().str.title()
    airports['iata_code'] = airports['iata_code'].str.strip().str.upper()
    airports['icao_code'] = airports['icao_code'].str.strip().str.upper()
    airports.drop(columns=['source'], inplace=True)
    airports['iata_code'].fillna('Unknown', inplace=True)
    airports["airport_type"].fillna('Unknown', inplace=True)
    airports['icao_code'].fillna('Unknown', inplace=True)
    airports['timezone_offset'].fillna('Unknown offset', inplace=True)
    airports['city'].fillna('Unknown', inplace=True)
    airports["time_zone_identifier"].fillna('Unknown zone identifier', inplace=True)
    airports["timezone_offset"].fillna('Unknown', inplace=True)
    airports['geom'] = 'POINT(' + airports['longitude'].astype(str) + ' ' + airports['latitude'].astype(str) + ')'

    airports.to_csv('.\data\transformed_data\airports.csv', index=False)

    return airports
def transform_airlines(airlines):
    airlines.replace('\\N', np.nan, inplace=True)
    airlines.drop(index=0, inplace=True)
     # Strip whitespace and standardize case
    airlines['country'] = airlines['country'].str.strip().str.title()
    airlines['name'] = airlines['name'].str.strip()
    # Uppercase codes
    airlines['iata_code'] = airlines['iata_code'].str.upper()
    airlines['icao_code'] = airlines['icao_code'].str.upper()
    airlines['alias'].fillna('Unknown', inplace=True)
    airlines['iata_code'].fillna('Unknown', inplace=True)
    airlines['icao_code'].fillna('Unknown', inplace=True)
    airlines['callsign'].fillna('Unknown', inplace=True)
    airlines['country'].fillna('Unknown', inplace=True)
   
    airlines.to_csv('.\data\transformed_data\airlines.csv', index=False)
    return airlines
def transform_routes(routes, airports,airlines):
    routes.replace('\\N', np.nan, inplace=True)
    routes.dropna(subset=['flight_number',"source_airport_id","destination_airport_id"], inplace=True)
    
    routes = routes.merge(
    airports[['iata_code', 'latitude', 'longitude']],
    left_on='source_airport',
    right_on='iata_code',
    how='left').rename(columns={'latitude': 'source_latitude', 'longitude': 'source_longitude'})

    routes = routes.merge(
    airports[['iata_code', 'latitude', 'longitude']],
    left_on='destination_airport',
    right_on='iata_code',
    how='left').rename(columns={'latitude': 'dest_latitude', 'longitude': 'dest_longitude'})
    
    def calculate_distance(row):
    # Check if any coordinate is NaN
        if np.isnan(row['source_latitude']) or np.isnan(row['source_longitude']) or \
        np.isnan(row['dest_latitude']) or np.isnan(row['dest_longitude']):
            return np.nan  # Return NaN if any coordinate is missing
        else:
            coords_1 = (row['source_latitude'], row['source_longitude'])
            coords_2 = (row['dest_latitude'], row['dest_longitude'])
            return great_circle(coords_1, coords_2).kilometers
        

# Apply the function without dropping rows
    routes['distance_km'] = routes.apply(calculate_distance, axis=1)
    routes.dropna(subset=['source_longitude', 'source_latitude'], inplace=True)
    routes.dropna(subset=['dest_longitude', 'dest_latitude'], inplace=True)

    routes=routes.loc[:,[
    'airline_code',      # Code of the airline
    'flight_number',     # Flight number associated with the route    # IATA code of the source airport
    'source_airport_id', # Unique identifier for the source airport
    'destination_airport_id', # Unique identifier for the destination airport
    'codeshare',         # Codeshare indicator (if applicable)
    'stopovers',         # Number of stopovers (0 for direct flights)
    'equipment_type' ,
    "distance_km"
            # Type of aircraft used for the flight
]]
    
    routes["equipment_type"].fillna("Unknown", inplace=True)    
    routes['codeshare']=routes['codeshare'].notna()
    routes = routes.merge(
    airlines[(airlines["active"] == "Y") & (airlines["iata_code"] != "Unknown")],
    left_on='airline_code',
    right_on='iata_code',
    how='left').rename(columns={'id':'airline_id'})[['airline_id','flight_number','source_airport_id','destination_airport_id','codeshare','stopovers','equipment_type',"distance_km"]]
    routes.dropna(subset=['airline_id'], inplace=True)
    routes['airline_id'] = routes['airline_id'].astype(int)
    routes.to_csv('.\data\transformed_data\routes.csv', index=False)
    # Get unique airline codes from routes
    # unique_airline_codes = routes['airline_code'].unique()
    # unique_iata_codes = airlines['iata_code'].unique()
    # missing_codes = [code for code in unique_airline_codes if code not in unique_iata_codes]
    # routes=routes[~routes['airline_code'].isin(missing_codes)]

    return routes


def transform_data(airports, airlines, routes):
    airports = transform_airports(airports)
    airlines = transform_airlines(airlines)
    routes = transform_routes(routes, airports,airlines)
    return airports, airlines, routes

    








