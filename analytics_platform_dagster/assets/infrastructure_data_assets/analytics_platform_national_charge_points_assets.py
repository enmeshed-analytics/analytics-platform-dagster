"""
Need to finish
"""

# from dagster import asset, AssetExecutionContext
# from ...utils.requests_helper import return_api_data
# from ...models.infrastructure_data_models.national_charge_point_model import ChargeDeviceResponse


# API_ENDPOINT = "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/ozev-ukpn-national-chargepoint-register/records?limit=10"

# @asset
# def fetch_data(context: AssetExecutionContext) -> ChargeDeviceResponse:
#     url = API_ENDPOINT
#     data = return_api_data(url)
#     return ChargeDeviceResponse.model_validate(data, strict=True)

# def normalise_charge_point_data(context: AssetExecutionContext, fetch_data):
