import requests
import pandas as pd

from dagster import asset, AssetIn, AssetExecutionContext
from ...models.environment_data_models.ea_flood_areas_model import EaFloodAreasResponse
from ...utils.slack_messages.slack_message import with_slack_notification
from ...utils.variables_helper.url_links import asset_urls

API_ENDPOINT = asset_urls.get("ea_flood_areas")

@asset(group_name="environment_data", io_manager_key="S3Json")
def ea_flood_areas_bronze(context: AssetExecutionContext):
    """
    EA Flood Area data bronze bucket
    """

    if API_ENDPOINT:
        try:
            url = API_ENDPOINT
            if url:
                response = requests.get(url)
                response.raise_for_status()
                result = response.content
                EaFloodAreasResponse.model_validate_json(result)
                context.log.info(f"Model Validated. Validated {len(result)} records")
                data = response.json()
                return data
        except Exception as error:
            raise error

@asset(
    group_name="environment_data",
    io_manager_key="DeltaLake",
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

    if data:
        try:
            items = data["items"]
            df = pd.DataFrame(items)
            context.log.info(f"Success: {df.head(25)}, {df.columns}, {df.shape}")
            context.log.info(f"Success: {df.columns}, {df.shape}")
            context.log.info(f"Success: {df.shape}")
            return df
        except Exception as e:
            raise e
