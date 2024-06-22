import requests
import duckdb
import pandas as pd
import zipfile
import tempfile
import os
import time
import fiona

from utils.duckdb_helper import process_chunk
from shapely import wkt
from shapely.geometry import shape
from loguru import logger
from dagster import asset, OpExecutionContext

def wait_10_seconds(context: OpExecutionContext):
    time.sleep(10)
    context.log.info("Waited for 10 seconds")

@asset
def dbt_trade_barriers(context: OpExecutionContext):
    # Get data from api and create dataframe
    url = 'https://data.api.trade.gov.uk/v1/datasets/market-barriers/versions/latest/data?format=json'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data['barriers'])

    # Create a connection to a persistent DuckDB database file named "data"
    con = duckdb.connect('data.duckdb')

    # Create a table and insert the data
    con.execute('CREATE TABLE IF NOT EXISTS trade_barriers AS SELECT * FROM df')
    print("Data stored in the 'trade_barriers' table.")
    wait_10_seconds(context)

@asset(deps=[dbt_trade_barriers])
def ea_flood_areas(context: OpExecutionContext):
    # Get data from api and create dataframe
    url = 'https://environment.data.gov.uk/flood-monitoring/id/floodAreas?_limit=9999'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data['items'])
    print(df.head())

    # Create a connection to a persistent DuckDB database file named "data"
    con = duckdb.connect('data.duckdb')

    # Create a table and insert the data
    con.execute('CREATE TABLE IF NOT EXISTS ea_flood_areas AS SELECT * FROM df')
    print("Data stored in the 'ea_flood_areas' table.")
    wait_10_seconds(context)

@asset(deps=[ea_flood_areas])
def ea_floods(context: OpExecutionContext):
    # Get data from api and create dataframe
    url = 'https://environment.data.gov.uk/flood-monitoring/id/floods'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data['items'])
    print(df.head())

    # Create a connection to a persistent DuckDB database file named "data"
    con = duckdb.connect('data.duckdb')

    # Create a table and insert the data
    con.execute('CREATE TABLE IF NOT EXISTS ea_floods AS SELECT * FROM df')
    print("Data stored in the 'ea_floods' table.")
    wait_10_seconds(context)

@asset(deps=[ea_floods])
def os_open_usrns(context: OpExecutionContext):
    db_file = 'data.duckdb'
    chunk_size = 50000
    url = "https://api.os.uk/downloads/v1/products/OpenUSRN/downloads?area=GB&format=GeoPackage&redirect"

    try:
        # Step 1: Retrieve the redirect URL
        response = requests.get(url)
        response.raise_for_status()
        redirect_url = response.url
        logger.success(f"Success! Redirect URL is: {redirect_url}")

        # Step 2: Download the content from the redirect URL
        response = requests.get(redirect_url)
        response.raise_for_status()
        zip_content = response.content
        logger.success("Zip file downloaded")

        # Step 3: Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Step 4: Write the zip file to the temporary directory
            zip_path = os.path.join(temp_dir, 'temp.zip')
            with open(zip_path, 'wb') as zip_file:
                zip_file.write(zip_content)

            # Step 5: Extract the contents of the zip file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # Step 6: Find the GeoPackage file
            gpkg_file = None
            for file_name in os.listdir(temp_dir):
                if file_name.endswith('.gpkg'):
                    gpkg_file = os.path.join(temp_dir, file_name)
                    break

            if gpkg_file:
                try:
                    # Create a DuckDB connection
                    with duckdb.connect(database=db_file, read_only=False) as conn:
                        # Create the table if it doesn't exist
                        conn.execute("""
                            CREATE TABLE OR REPLACE open_usrns_table (
                                geometry VARCHAR,
                                street_type VARCHAR,
                                usrn INTEGER
                            )
                        """)
                        # Process the GeoPackage in chunks
                        with fiona.open(gpkg_file, 'r') as src:
                            features = []
                            for i, feature in enumerate(src):
                                try:
                                    # Convert geometry to WKT string
                                    geom = shape(feature['geometry'])
                                    feature['properties']['geometry'] = wkt.dumps(geom)
                                except Exception as e:
                                    feature['properties']['geometry'] = None
                                    logger.warning(f"Error converting geometry for feature {i}: {e}")
                                features.append(feature['properties'])
                                if len(features) == chunk_size:
                                    # Process the chunk
                                    df_chunk = pd.DataFrame(features)
                                    process_chunk(df_chunk, conn)
                                    logger.info(f"Processed features {i-chunk_size+1} to {i}")
                                    features = []
                            # Process any remaining features
                            if features:
                                df_chunk = pd.DataFrame(features)
                                process_chunk(df_chunk, conn)
                                logger.info(f"Processed remaining features up to {i}")
                    logger.success("Data loaded into DuckDB successfully")
                except Exception as e:
                    logger.error(f"Error processing GeoPackage: {e}")
                    raise
            else:
                raise FileNotFoundError("No GeoPackage file found in the zip archive")
    except Exception as e:
        logger.error(f"An error ocurred: {e}")
        raise
    wait_10_seconds(context)

# @asset(deps=[ea_floods])
# def london_data_store_meta():
#     """
#     Currently testing this to figure out the nested array structure in the dataframe columns
#     """
#     # Get data from api and create dataframe
#     url = 'https://data.london.gov.uk/data.json'
#     response = requests.get(url)
#     response.raise_for_status()
#     data = response.json()
#     df = pd.DataFrame(data)
#     df.to_parquet("df")