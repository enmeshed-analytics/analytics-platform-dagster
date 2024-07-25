import requests
import pandas as pd

from ...utils.url_links import asset_urls
from ...utils.io_manager import AwsWranglerDeltaLakeIOManager
from dagster import asset, OpExecutionContext, Output

@asset(group_name="metadata_assets")
def london_datastore(context: OpExecutionContext):
    """
    London Datastore Metadata
    """
    try:
        url = asset_urls.get("london_data_store")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        df = df.astype(str)
        delta_io = AwsWranglerDeltaLakeIOManager("analytics-data-lake-bronze")
        return Output(delta_io.handle_output(context, df))
    except Exception as error:
        context.log.error(f"Error in dbt_trade_barriers: {str(error)}")
        raise error