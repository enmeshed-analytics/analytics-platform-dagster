import requests
import pandas as pd

from io import BytesIO
from pydantic import TypeAdapter, ValidationError
from typing import List
from ...models.infrastructure_data_models.ukpn_live_fault_model import UKPNLiveFault
from dagster import AssetExecutionContext, AssetIn, asset, op
from datetime import datetime
from ...utils.slack_messages.slack_message import with_slack_notification
from ...utils.variables_helper.url_links import asset_urls

@op
def validate_model(fault_data):
    """
    Validate json against pydantic model
    """
    try:
        # Validate the datamodel
        adapter = TypeAdapter(List[UKPNLiveFault])
        adapter.validate_python(fault_data)
    except ValidationError as e:
        print("Validation errors:")
        for error in e.errors():
            print(f"Field: {error['loc']}, Error: {error['msg']}")
        raise

@asset(group_name="infrastructure_assets", io_manager_key="S3Parquet")
def ukpn_live_faults_bronze():
    try:
        # Make request
        url = asset_urls.get("ukpn_live_faults")
        if url is None:
            raise ValueError("No url!")

        params = {
            "lang": "en",
            "timezone": "Europe/London",
            "use_labels": "true"
        }
        response = requests.get(url, params=params)
        response.raise_for_status()

        # Create Bytes object
        bytes_io = BytesIO(response.content)
        df = pd.read_excel(bytes_io)

        # Convert DataFrame to list of dictionaries
        data_list = df.to_dict(orient='records')
    except Exception as e:
        raise e

    try:
        # Validate the datamodel
        validate_model(data_list)
    except Exception as e:
        raise e

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow")
    df = df.astype(str)
    parquet_bytes = parquet_buffer.getvalue()

    return parquet_bytes

@asset(
    group_name="infrastructure_assets",
    io_manager_key="DeltaLake",
    metadata={"mode": "overwrite"},
    ins={"ukpn_live_faults_bronze": AssetIn("ukpn_live_faults_bronze")},
    required_resource_keys={"slack"}
)
@with_slack_notification("UKPN Live Fault Data")
def ukpn_live_faults_silver(context: AssetExecutionContext, ukpn_live_faults_bronze):
    """
    Store carbon intensity data in Delta Lake.

    Rename columns and add additonal information.
    """
    data = ukpn_live_faults_bronze

    # Add date_processed column
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    data["date_processed"] = current_time

    # Check info
    context.log.info(f"{data.head(10)}")
    context.log.info(f"{data.columns}")
    return data
