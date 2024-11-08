import requests
import polars as pl
import io
from pydantic import ValidationError

from ...utils.variables_helper.url_links import asset_urls
from dagster import AssetExecutionContext, asset, AssetIn
from ...utils.slack_messages.slack_message import with_slack_notification
from ...models.environment_data_models.green_belt_model import GreenBeltResponse

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
        print(data)
        try:
            GreenBeltResponse.model_validate(data)
        except ValidationError as e:
            validation_errors = e.errors()

        entities = data['entities']

        df = pl.DataFrame(entities)
        context.log.info(f"Processed {len(df)} records with {len(validation_errors)} validation errors")

        parquet_buffer = io.BytesIO()
        df.write_parquet(parquet_buffer)
        parquet_bytes = parquet_buffer.getvalue()

        context.log.info("Successfully processed batch into Parquet format")
        return parquet_bytes

    except Exception as e:
        context.log.error(f"Error processing data: {str(e)}")
        raise e


@asset(
    group_name="environment_data",
    io_manager_key="PolarsDeltaLake",
    metadata={"mode": "overwrite"},
    ins={
        "green_belt_bronze": AssetIn(
            "green_belt_bronze"
        )
    },
    required_resource_keys={"slack"}
)
@with_slack_notification("GB Green Belt")
def green_belt_silver(context: AssetExecutionContext, green_belt_bronze)  -> pl.DataFrame:
    """
    Write green belt data directly to S3 using Polars
    """
    df = pl.DataFrame(green_belt_bronze)
    return df
