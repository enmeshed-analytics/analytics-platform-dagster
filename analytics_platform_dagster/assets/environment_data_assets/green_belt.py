import requests
import pyarrow as pa
import pandas as pd
import io

from typing import List, Dict, Any
from pydantic import ValidationError
from ...utils.variables_helper.url_links import asset_urls
from dagster import AssetExecutionContext, AssetIn, asset
from ...utils.slack_messages.slack_message import with_slack_notification
from ...models.environment_data_models.green_belt_model import (
    GreenBeltResponse,
)

@asset(
    group_name="environment_data",
    io_manager_key="S3Parquet"
)
def green_belt_bronze(context: AssetExecutionContext):
    """
    Write green belt data out to raw staging area

    Returns:
        Parquet in S3.
    """
    url = asset_urls.get("green_belt")

    if url is None:
        raise ValueError("No url!")

    validation_errors = []
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        try:
            GreenBeltResponse.model_validate(data)
        except ValidationError as e:
            validation_errors = e.errors()

        df = pd.DataFrame(data)
        df = df.astype(str)
        context.log.info(f"Processed {len(df)} records with {len(validation_errors)} validation errors")

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow")
        parquet_bytes = parquet_buffer.getvalue()

        context.log.info("Successfully processed batch into Parquet format")
        return parquet_bytes

    except Exception as e:
        context.log.error(f"Error processing data: {str(e)}")
        raise e

@asset(
    group_name="environment_data",
    io_manager_key="DeltaLake",
    metadata={"mode": "overwrite"},
    ins={
        "green_belt_bronze": AssetIn(
            "green_belt_bronze"
        )
    },
    required_resource_keys={"slack"}
)
@with_slack_notification("GB Green Belt")
def green_belt_silver(
    context: AssetExecutionContext, green_belt_bronze
) -> pd.DataFrame:
    """
    Write green belt data out to Delta Lake

    Returns:
        Delta Lake table in S3.
    """
    try:
        df = pd.DataFrame(green_belt_bronze)
        return df

    except Exception as e:
        context.log.error(f"Error processing data: {e}")
        raise
