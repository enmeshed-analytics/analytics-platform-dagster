import aiohttp
import asyncio
import pandas as pd
import io

from datetime import datetime
from dagster import asset, AssetExecutionContext, AssetIn
from ...models.energy_data_models.carbon_intensity_assets_model import (
    CarbonIntensityResponse,
)
from ...utils.variables_helper.url_links import asset_urls
from ...utils.slack_messages.slack_message import with_slack_notification

async def fetch_data(
    session: aiohttp.ClientSession, region_id: int
) -> CarbonIntensityResponse:
    """
    Async function to fetch data and validate against model.

    This calls the API end point for all available region_id numbers
    """
    base = asset_urls.get("carbon_intensity_api")
    if base is None:
        raise ValueError("Could not load base url")

    url = f"{base}{region_id}"
    async with session.get(url) as response:
        data = await response.json()
        return CarbonIntensityResponse.model_validate(data, strict=True)


async def fetch_all_data_async():
    """
    Fetch ID for 18 region_ids.

    Put into parquet format.
    """
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, region_id) for region_id in range(1, 18)]
        results = await asyncio.gather(*tasks)

    # List to store data
    data = []

    # Access data fields in model using dot notation
    for result in results:
        region = result.data[0]
        region_data = {
            "regionid": region.regionid,
            "dnoregion": region.dnoregion,
            "shortname": region.shortname,
            "from": region.data[0].from_,
            "to": region.data[0].to,
            "intensity_forecast": region.data[0].intensity.forecast,
            "intensity_index": region.data[0].intensity.index,
        }

        # Access nested data fields
        for item in region.data[0].generationmix:
            region_data[f"generationmix_{item.fuel}"] = item.perc
        data.append(region_data)

    df = pd.DataFrame(data)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_bytes = parquet_buffer.getvalue()

    return parquet_bytes


@asset(group_name="energy_assets", io_manager_key="S3Parquet")
def carbon_intensity_bronze(context: AssetExecutionContext):
    """
    Store carbon intensity data in Parquet in S3...

    Dump to bronze bucket.
    """
    data = asyncio.run(fetch_all_data_async())
    return data


@asset(
    group_name="energy_assets",
    io_manager_key="DeltaLake",
    metadata={"mode": "append"},
    ins={"carbon_intensity_bronze": AssetIn("carbon_intensity_bronze")},
    required_resource_keys={"slack"}
)
@with_slack_notification("UK Regional Carbon Intensity Data")
def carbon_intensity_silver(context: AssetExecutionContext, carbon_intensity_bronze):
    """
    Store carbon intensity data in Delta Lake.

    Rename columns and add additonal information.
    """
    data = carbon_intensity_bronze
    data = data.rename(
        columns={
            "regionid": "region_id",
            "dnoregion": "dno_region",
            "shortname": "short_name",
            "from": "start_period",
            "to": "end_period",
        }
    )

    # Add date_processed column
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    data["date_processed"] = current_time

    # Add asset_group column
    asset_group = context.assets_def.group_names_by_key[context.asset_key]
    context.log.info(f"Asset group: {asset_group}")
    data["asset_group"] = asset_group

    # Print info
    context.log.info(f"{data.head(10)}")
    context.log.info(f"{data.columns}")
    return data
