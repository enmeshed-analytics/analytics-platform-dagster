import requests
import polars as pl
import io

from pydantic import ValidationError
from dagster import asset, AssetIn, AssetExecutionContext
from ...models.environment_data_models.ea_flood_areas_model import EaFloodAreasResponse
from ...utils.slack_messages.slack_message import with_slack_notification
from ...utils.variables_helper.url_links import asset_urls



@asset(group_name="environment_data", io_manager_key="S3Parquet")
def ea_flood_areas_bronze(context: AssetExecutionContext):
    """
    EA Flood Area data bronze bucket
    """

    try:
        url = asset_urls.get("ea_flood_areas")
        if url is None:
            raise ValueError("URL for london_data_store is not found in asset_urls")

        response = requests.get(url)
        response.raise_for_status()
        result = response.json()

        validation_errors = []
        try:
            EaFloodAreasResponse.model_validate_json(result)
        except ValidationError as e:
            validation_errors = e.errors()

        data = result["items"]
        df = pl.DataFrame(data)

        context.log.info(f"Processed {len(df)} records with {len(validation_errors)} validation errors")

        parquet_buffer = io.BytesIO()
        df.write_parquet(parquet_buffer)
        parquet_bytes = parquet_buffer.getvalue()

        context.log.info("Successfully processed Parquet into Bronze bucket")
        return parquet_bytes
    except Exception as error:
        raise error

@asset(
    group_name="environment_data",
    io_manager_key="PolarsDeltaLake",
    metadata={"mode": "overwrite"},
    ins={"ea_flood_areas_bronze": AssetIn("ea_flood_areas_bronze")},
    required_resource_keys={"slack"}
)
@with_slack_notification("Environment Agency Flood Area Data")
def ea_flood_areas_silver(context: AssetExecutionContext, ea_flood_areas_bronze):
    """
    EA Flood Area data silver bucket
    """

    data = ea_flood_areas_bronze

    try:
        df = pl.DataFrame(data)
        context.log.info(f"Success: {df.head(25)}, {df.columns}, {df.shape}")
        context.log.info(f"Success: {df.columns}, {df.shape}")
        context.log.info(f"Success: {df.shape}")
        return df
    except Exception as e:
        raise e
