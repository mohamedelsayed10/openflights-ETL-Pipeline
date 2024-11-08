# app.py

from create_schema import create
from extract import extract_data
from transform import transform_data
from load import load_data
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)


def run_etl_pipeline():
    # Step 1: Create schema
    create()
    print("scema created")

    # Step 2: Extract data
    airports, airlines, routes = extract_data()
    print("data extracted")

    # Step 3: Transform data
    airports, airlines, routes = transform_data(airports, airlines, routes)
    print("data transformed")

    # Step 4: Load data
    load_data(airports, airlines, routes)
    print("data loaded")
    print("ETL pipeline completed successfully!")


run_etl_pipeline()
