import os
from dagster import Definitions, load_assets_from_modules
from dagster_slack import SlackResource, make_slack_on_run_failure_sensor

from .assets.location_data_assets import analytics_platform_os_assets
from .assets.environment_data_assets import analytics_platform_ea_flood_areas
from .assets.trade_data_assets import analytics_platform_dbt_trade_barrier_assets
from .assets.energy_data_assets import (
    analytics_platform_carbon_intensity_assets,
    entsog_uk_gas_assets,
)
from .assets.catalogue_metadata_assets import analytics_platform_datastore_assets
from .assets.infrastructure_data_assets import (
    analytics_platform_national_charge_points_assets,
)

from .utils.io_manager_helper.io_manager import (
    S3ParquetManager,
    AwsWranglerDeltaLakeIOManager,
    S3JSONManager,
)

from .jobs.analytics_platfom_jobs import (
    environment_job_1,
    trade_job_1,
    trade_job_1_daily,
    metadata_job_1,
    energy_job_1,
    energy_job_1_daily,
    infrastructure_job_1,
)

slack_failure_sensor = make_slack_on_run_failure_sensor(
    slack_token=getenv("SLACKBOT"),
    channel="#pipelines",
)

defs = Definitions(
    assets=load_assets_from_modules(
        [
            analytics_platform_datastore_assets,
            analytics_platform_os_assets,
            analytics_platform_ea_flood_areas,
            analytics_platform_dbt_trade_barrier_assets,
            analytics_platform_carbon_intensity_assets,
            analytics_platform_national_charge_points_assets,
            entsog_uk_gas_assets,
        ]
    ),
    jobs=[
        environment_job_1,
        trade_job_1,
        metadata_job_1,
        energy_job_1,
        infrastructure_job_1,
    ],
    schedules=[energy_job_1_daily, trade_job_1_daily],
    sensors=[slack_failure_sensor],
    resources={
        "S3Parquet": S3ParquetManager(
            bucket_name=os.getenv("BRONZE_DATA_BUCKET")
        ),
        "S3Json": S3JSONManager(
            bucket_name=os.getenv("BRONZE_DATA_BUCKET")
        ),
        "DeltaLake": AwsWranglerDeltaLakeIOManager(
            bucket_name=os.getenv("SILVER_DATA_BUCKET")
        ),
        "slack": SlackResource(token=os.getenv("SLACKBOT")),
    },
)
