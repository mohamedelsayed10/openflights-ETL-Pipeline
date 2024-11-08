# app.py

from create_schema import create
from extract import extract_data
from transform import transform_data
from load import load_data

def run_etl_pipeline():
    # Step 1: Create schema
    create()

    # Step 2: Extract data
    airports, airlines, routes = extract_data()

    # Step 3: Transform data
    airports, airlines, routes = transform_data(airports, airlines, routes)

    # Step 4: Load data
    load_data(airports, airlines, routes)

if __name__ == "__main__":
    run_etl_pipeline()
