import aiohttp
import asyncio
import pandas as pd

from dagster import asset, OpExecutionContext, Output
from ...utils.url_links import carbon_intensity_api_list
from ...models.energy_data_models.carbon_intensity_assets_model import CarbonIntensityResponse
from ...utils.io_manager import AwsWranglerDeltaLakeIOManager

API_ENDPOINT = "https://api.carbonintensity.org.uk/regional/regionid/"

async def fetch_data(session: aiohttp.ClientSession, region_id: int) -> CarbonIntensityResponse:
    url = f"{API_ENDPOINT}{region_id}"
    async with session.get(url) as response:
        data = await response.json()
        return CarbonIntensityResponse.model_validate(data, strict=True)

async def fetch_all_data_async():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, region_id) for region_id in carbon_intensity_api_list]
        results = await asyncio.gather(*tasks)
    
    data = []
    for result in results:
        region = result.data[0]
        region_data = {
            'regionid': region.regionid,
            'dnoregion': region.dnoregion,
            'shortname': region.shortname,
            'from': region.data[0].from_,
            'to': region.data[0].to,
            'intensity_forecast': region.data[0].intensity.forecast,
            'intensity_index': region.data[0].intensity.index
        }
        for item in region.data[0].generationmix:
            region_data[f"generationmix_{item.fuel}"] = item.perc
        data.append(region_data)
    
    return pd.DataFrame(data)

@asset(group_name="energy_assets")
def fetch_carbon_data():
    return asyncio.run(fetch_all_data_async())

@asset(group_name="energy_assets")
def carbon_intensity_delta_lake(context: OpExecutionContext, fetch_carbon_data: pd.DataFrame):
    """
    Store carbon intensity data in Delta Lake.
    """
    try:
        delta_io = AwsWranglerDeltaLakeIOManager("analytics-data-lake-bronze")
        result = delta_io.handle_output(context, fetch_carbon_data)
        return Output(result)
    except Exception as error:
        context.log.error(f"Error in carbon_intensity_delta_lake: {str(error)}")
        raise