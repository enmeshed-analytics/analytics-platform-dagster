import json
import requests
import duckdb
from dagster import asset
import pandas as pd

@asset
def dbt_trade_barriers():
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
