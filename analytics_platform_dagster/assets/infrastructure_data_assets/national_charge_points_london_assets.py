"""
This currently only provides data for London only
"""
import polars as pl
import io

from pydantic import ValidationError
from ...utils.requests_helper.requests_helper import return_json
from ...utils.variables_helper.url_links import asset_urls
from ...models.infrastructure_data_models.national_charge_point_london_model import (
    ChargeDevice,
)
from dagster import AssetExecutionContext, AssetIn, asset
from ...utils.slack_messages.slack_message import with_slack_notification

@asset(group_name="infrastructure_assets", io_manager_key="S3Parquet")
def national_charge_point_london_bronze(context: AssetExecutionContext):
    """
    Fetches json data and uploads charge point data to S3 using IO Manager

    Returns:
        A parquet file in S3
    """

    # Fetch url
    url = asset_urls.get("national_charge_point_london")
    if url is None:
        raise ValueError("API_ENDPOINT can't be None")

    response = return_json(url)

    validation_errors = []
    try:
        ChargeDevice.model_validate(response)
    except ValidationError as e:
        validation_errors = e.errors()

    df = pl.DataFrame(response, infer_schema_length=None)
    df = df.select([pl.col("*").cast(pl.Utf8)])

    context.log.info(f"Processed {len(df)} records with {len(validation_errors)} validation errors")

    parquet_buffer = io.BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_bytes = parquet_buffer.getvalue()

    context.log.info("Successfully processed Parquet into bronze bucket")
    return parquet_bytes

@asset(
    group_name="infrastructure_assets",
    io_manager_key="PolarsDeltaLake",
    metadata={"mode": "overwrite"},
    ins={
        "national_charge_point_london_bronze": AssetIn(
            "national_charge_point_london_bronze"
        )
    },
    required_resource_keys={"slack"}
)
@with_slack_notification("National EV Charge Point Data London")
def national_charge_point_london_silver(
    context: AssetExecutionContext, national_charge_point_london_bronze
) -> pl.DataFrame:
    """
    Write charge point data out to Delta Lake

    Returns:
        Delta Lake table in S3.
    """
    try:
        df = pl.DataFrame(national_charge_point_london_bronze)
        return df

    except Exception as e:
        context.log.error(f"Error processing data: {e}")
        raise
