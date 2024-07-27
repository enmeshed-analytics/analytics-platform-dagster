# from ...utils.requests_helper import return_api_data_json
# from ...utils.io_manager import S3JSONManager
# from ...models.infrastructure_data_models.national_charge_point_model import ChargeDeviceResponse
# from datetime import datetime
# from dagster import asset, AssetExecutionContext, AssetIn

# API_ENDPOINT = "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/ozev-ukpn-national-chargepoint-register/records?limit=50"

# def fetch_national_charge_point_data() -> ChargeDeviceResponse:
#     url = API_ENDPOINT
#     data = return_api_data_json(url)
#     return ChargeDeviceResponse.model_validate(data, strict=True)

# @asset(
#     group_name="energy_assets", 
#     io_manager_key="S3Parquet"
#     )
# def national_charge_point_data_bronze():
#     response = fetch_national_charge_point_data()
#     return response


# def national_charge_point_data_silver():
#     # Fetch the data
#     response = fetch_national_charge_point_data()
    
#     # Extract the results
#     charge_devices = response.results
    
#     # Convert to a list of dictionaries
#     data = [device.model_dump() for device in charge_devices]
    
#     # Create a DataFrame
#     df = pd.DataFrame(data)
    
#     # Handle the nested geo_point structure
#     df['lon'] = df['geo_point'].apply(lambda x: x['lon'])
#     df['lat'] = df['geo_point'].apply(lambda x: x['lat'])
#     df = df.drop('geo_point', axis=1)
    
#     return df

var = national_charge_point_data_bronze()