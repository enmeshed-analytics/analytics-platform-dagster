import requests
import duckdb
import pandas as pd
import time

from loguru import logger
from dagster import asset, OpExecutionContext

def wait_10_seconds(context: OpExecutionContext):
    time.sleep(10)
    context.log.info("Waited for 10 seconds")

@asset
def ea_flood_areas(context: OpExecutionContext):
    """
    tbc
    """
    try:
        # Get data from api and create dataframe
        url = 'https://environment.data.gov.uk/flood-monitoring/id/floodAreas?_limit=9999'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data['items'])
        assert df is not None and not df.empty, "DataFrame is empty or None"
    except requests.RequestException:
        raise

    try:
        # Create a connection to a persistent DuckDB database file named "data"
        con = duckdb.connect('data.duckdb')

        # Create a table and insert the data
        con.execute('CREATE TABLE IF NOT EXISTS ea_flood_areas AS SELECT * FROM df')
        logger.success("Data stored in the 'ea_flood_areas' table.")
    except duckdb.Error:
        raise

    wait_10_seconds(context)

@asset(deps=[ea_flood_areas])
def ea_floods(context: OpExecutionContext):
    """
    tbc
    """
    try:

        # Get data from api and create dataframe
        url = 'https://environment.data.gov.uk/flood-monitoring/id/floods'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data['items'])
        assert df is not None and not df.empty, "DataFrame is empty or None"
    except requests.RequestException:
        raise

    try:
        # Create a connection to a persistent DuckDB database file named "data"
        con = duckdb.connect('data.duckdb')

        # Create a table and insert the data
        con.execute('CREATE TABLE IF NOT EXISTS ea_floods AS SELECT * FROM df')
        logger.success("Data stored in the 'ea_floods' table.")
    except duckdb.Error:
        raise
    wait_10_seconds(context)
