
from typing import Any
from ...utils.requests_helper import stream_json
from ...utils.url_links import asset_urls
# from ...models.infrastructure_data_models.national_charge_point_model import ChargeDeviceResponse
from dagster import AssetExecutionContext, asset

DONWLOADLINK = asset_urls.get("national_charge_points")

def fetch_national_charge_point_data() -> Any:
    try:
        url = DONWLOADLINK
        return stream_json(url, set_chunk=5000)
    except Exception as error:
        raise error

@asset(
    group_name="infrastructure_assets",
    io_manager_key="S3Json" 
    )
def national_charge_point_data_bronze(context: AssetExecutionContext) -> Any:
    context.log.info("Started Processing Data")
    response = fetch_national_charge_point_data()
    context.log.info("Finished Processing Data")
    return response
