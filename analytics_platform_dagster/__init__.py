from dagster import Definitions, load_assets_from_modules
from .assets.location_data_assets import analytics_platform_os_assets
from .assets.environment_data_assets import analytics_platform_ea_assets
from .assets.trade_data_assets import analytics_platform_dbt_assets
from .assets.energy_data_assets import analytics_platform_carbon_intensity_assets
from .assets.catalogue_metadata_assets import analytics_platform_datastore_assets
from .utils.io_manager import S3ParquetManager, AwsWranglerDeltaLakeIOManager

from .jobs.analytics_platfom_jobs import (
    environment_job_1, 
    trade_job_1, 
    metadata_job_1,
    energy_job_1,
    energy_job_1_daily
)

defs = Definitions(
    assets=load_assets_from_modules([
        analytics_platform_datastore_assets, 
        analytics_platform_os_assets,
        analytics_platform_ea_assets, 
        analytics_platform_dbt_assets, 
        analytics_platform_carbon_intensity_assets
    ]),

    jobs=[
    environment_job_1, 
    trade_job_1, 
    metadata_job_1,
    energy_job_1,
    ], 
    schedules=[energy_job_1_daily],
    resources={
        "S3Parquet": S3ParquetManager(bucket_name="datastackprod-bronzedatabucket85c612b2-tjqgl6ahaks5"),
        "DeltaLake": AwsWranglerDeltaLakeIOManager(bucket_name="datastackprod-silverdatabucket04c06b24-mrfdumn6njwe")
    },
)
