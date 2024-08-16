import requests
import pandas as pd

from dagster import asset, AssetIn, AssetExecutionContext
from ...models.environment_data_models.ea_flood_areas_model import EaFloodAreasResponse

@asset(group_name="environment_data", io_manager_key="S3Json")
def ea_flood_areas_bronze(context: AssetExecutionContext):
    """
    EA Flood Area data bronze bucket
    """
    try:
        url = "https://environment.data.gov.uk/flood-monitoring/id/floodAreas?_limit=9999"
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
)
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
