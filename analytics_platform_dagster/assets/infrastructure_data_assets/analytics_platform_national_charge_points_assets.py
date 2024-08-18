import pandas as pd

from pydantic import ValidationError
from typing import List, Dict, Any
from ...utils.requests_helper.requests_helper import stream_json, return_json
from ...utils.variables_helper.url_links import asset_urls
from ...models.infrastructure_data_models.national_charge_point_model import (
    ChargeDevice,
)
from dagster import AssetExecutionContext, AssetIn, asset
from ...utils.slack_messages.slack_message import with_slack_notification

DONWLOAD_LINK = asset_urls.get("national_charge_points")
API_ENDPOINT = asset_urls.get("national_charge_points_api")


def fetch_record_count() -> int:
    """Returns sample of charge point data to verify total count

    Returns:
            int: The total count of records
    """

    if API_ENDPOINT is None:
        raise ValueError("API_ENDPOINT can't be None")

    try:
        api_endpoint = API_ENDPOINT
        json_data = return_json(api_endpoint)
        total_count = json_data.get("total_count")
        return total_count
    except Exception as error:
        raise error


def fetch_national_charge_point_data() -> List[Dict[str, Any]]:
    """Streams charge point data into memory

    Returns:
        A list of dictionaries
    """

    if DONWLOAD_LINK is None:
        raise ValueError("Download Link can't be None")

    try:
        url = DONWLOAD_LINK
        return stream_json(url, set_chunk=5000)
    except Exception as error:
        raise error


@asset(group_name="infrastructure_assets", io_manager_key="S3Json")
def national_charge_point_data_bronze(context: AssetExecutionContext) -> List[Dict]:
    """Uploads charge point data to S3 using IO Manager.

    Fetches json data and compares with total record count to ensure that the correct amount of records was fetched.

    Returns:
        A json file in S3
    """

    response = fetch_national_charge_point_data()
    total_record_count = fetch_record_count()
    response_count = len(response)

    # Compare counts using sets.
    # This checks that all the data was returned and we haven't double counted and/or lost records.
    if set([total_record_count]) == set([response_count]):
        context.log.info(f"Record counts match: {total_record_count}")
    else:
        context.log.warning(
            f"Record count mismatch: Expected {total_record_count}, got {response_count}"
        )

    return response


@asset(
    group_name="infrastructure_assets",
    io_manager_key="DeltaLake",
    metadata={"mode": "overwrite"},
    ins={
        "national_charge_point_data_bronze": AssetIn(
            "national_charge_point_data_bronze"
        )
    },
)
@with_slack_notification("National EV Charge Point Data")
def national_charge_point_data_silver(
    context: AssetExecutionContext, national_charge_point_data_bronze
) -> pd.DataFrame:
    """Write charge point data out to Delta Lake once Pydantic models has been validated

    Returns:
        Delta Lake table in S3.

    """
    try:
        # Create DataFrame directly from input data
        df = pd.DataFrame(national_charge_point_data_bronze)

        # Validate a sample of 5% of the records against the Pydantic model
        sample_size = int(
            len(df) * 0.05
        )  # 5% of records - increase or lower if needed (0.01 would be 1% for exmaple)
        sample = df.sample(n=sample_size)

        for _, record in sample.iterrows():
            try:
                ChargeDevice.model_validate(record.to_dict())
            except ValidationError as e:
                context.log.warning(f"Validation error in sample: {e}")

        # Log some information about the DataFrame to check it all worked
        context.log.info(
            f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns"
        )
        context.log.info(f"DataFrame sample: {df.head(15)}")
        return df

    except Exception as e:
        context.log.error(f"Error processing data: {e}")
        raise
