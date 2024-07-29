import json

from typing import List, Dict
from ...utils.requests_helper.requests_helper import stream_json, return_json
from ...utils.variables_helper.url_links import asset_urls
# from ...models.infrastructure_data_models.national_charge_point_model import ChargeDeviceResponse
from dagster import AssetExecutionContext, asset

DONWLOAD_LINK = asset_urls.get("national_charge_points")
API_ENDPOINT = asset_urls.get("national_charge_points_api")

def fetch_record_count() -> json:
    """Returns sample of charge point data to verify total count
    
    Returns: 
        Json object
    """
    try:
        api_endpoint = API_ENDPOINT 
        json_data = return_json(api_endpoint)
        return json_data.get("total_count")
    except Exception as error:
        raise error

def fetch_national_charge_point_data() -> List[Dict]:
    """Streams charge point data into memory
    
    Returns: 
        A list of dictionaries
    """
    try:
        url = DONWLOAD_LINK
        return stream_json(url, set_chunk=5000)
    except Exception as error:
        raise error

@asset(
    group_name="infrastructure_assets",
    io_manager_key="S3Json" 
    )
def national_charge_point_data_bronze(context: AssetExecutionContext) -> List[Dict]:
    """ Uploads charge point data to S3 using IO Manager. 
    
    Fetches json data and compares with total record count to ensure that the correct amount of records was fetched. 
    
    Returns:
        A list of dictionaries
    """

    response = fetch_national_charge_point_data()
    total_record_count = fetch_record_count()
    response_count = len(response)
    
        # Compare counts using sets
    if set([total_record_count]) == set([response_count]):
        context.log.info(f"Record counts match: {total_record_count}")
    else:
        context.log.warning(f"Record count mismatch: Expected {total_record_count}, got {response_count}")

    return response

