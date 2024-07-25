import pandas as pd
import requests

from dagster import asset, OpExecutionContext, Output
from ...utils.url_links import asset_urls
from ...utils.io_manager import AwsWranglerDeltaLakeIOManager
from ...utils.trade_data_helper.dbt_trade_transform import normalise_data

@asset(group_name="trade_assets")
def dbt_trade_barriers(context: OpExecutionContext):
    """
    tbc
    """
    try:
        url = asset_urls.get("dbt_asset")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data['barriers'])
        return Output(df)
    except Exception as error:
        context.log.error(f"Error in dbt_trade_barriers: {str(error)}")
        raise error

@asset(group_name="trade_assets")
def dbt_trade_barriers_transform(context: OpExecutionContext, dbt_trade_barriers):
    """
    tbc
    """
    df = dbt_trade_barriers
    try:
        df = normalise_data(df)
        return Output(df)
    except Exception as error:
        context.log.error(f"Error in dbt_trade_barriers: {str(error)}")
        raise error

@asset(group_name="trade_assets")
def dbt_trade_barriers_delta_lake(context: OpExecutionContext, dbt_trade_barriers_transform):
    """
    tbc
    """
    df = dbt_trade_barriers_transform
    try:
        delta_io = AwsWranglerDeltaLakeIOManager("analytics-data-lake-bronze")
        return Output(delta_io.handle_output(context, df))
    except Exception as error:
        context.log.error(f"Error in dbt_trade_barriers: {str(error)}")
        raise error