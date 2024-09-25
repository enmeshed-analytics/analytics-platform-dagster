import os
from dagster import Definitions, load_assets_from_modules
from dagster_slack import SlackResource, make_slack_on_run_failure_sensor

from .assets.location_data_assets import analytics_platform_os_assets
from .assets.environment_data_assets import (
    analytics_platform_ea_flood_areas,
    analytics_platform_ea_flood_public_forecast,
)
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
    environment_job_1_daily,
    trade_job_1,
    trade_job_1_daily,
    metadata_job_1,
    metadata_job_1_weekly,
    energy_job_1,
    energy_job_1_daily,
    infrastructure_job_1,
    infrastructure_job_1_weekly,
)


def get_env_var(var_name: str) -> str:
    """
    Basic function to return environment variables with a proper error if None.

    Args:
        Var name

    Returs:
        Env var
    """
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Environment variable '{var_name}' is not set")
    return value


# Slack Failure message sensor
slack_failure_sensor = make_slack_on_run_failure_sensor(
    slack_token=get_env_var("SLACKBOT"),
    channel="#pipelines",
)

# Asset / job definitions as well as schedules / sensors / resources
defs = Definitions(
    assets=load_assets_from_modules(
        [
            analytics_platform_datastore_assets,
            analytics_platform_os_assets,
            analytics_platform_ea_flood_areas,
            analytics_platform_ea_flood_public_forecast,
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
    schedules=[
        energy_job_1_daily,
        environment_job_1_daily,
        trade_job_1_daily,
        infrastructure_job_1_weekly,
        metadata_job_1_weekly,
    ],
    sensors=[slack_failure_sensor],
    resources={
        "S3Parquet": S3ParquetManager(bucket_name=get_env_var("BRONZE_DATA_BUCKET")),
        "S3Json": S3JSONManager(bucket_name=get_env_var("BRONZE_DATA_BUCKET")),
        "DeltaLake": AwsWranglerDeltaLakeIOManager(
            bucket_name=get_env_var("SILVER_DATA_BUCKET")
        ),
        "slack": SlackResource(token=get_env_var("SLACKBOT")),
    },
)
