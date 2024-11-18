import os
from dagster import Definitions, load_assets_from_modules
from dagster_slack import SlackResource, make_slack_on_run_failure_sensor

from .assets.environment_data_assets import (
    analytics_platform_ea_flood_areas,
    analytics_platform_ea_flood_public_forecast,
    green_belt
)
from .assets.trade_data_assets import analytics_platform_dbt_trade_barrier_assets

from .assets.energy_data_assets import (
    analytics_platform_carbon_intensity_assets,
    entsog_uk_gas_assets,
    ukpn_smart_metres,
    renewable_energy_planning
)
from .assets.catalogue_metadata_assets import london_datastore, ukpn_datastore_roadmap

from .assets.infrastructure_data_assets import (
    national_charge_points_london_assets,
    uk_power_networks_live_faults,
    national_charge_points_uk_assets,
    uk_heat_networks
)

from .assets.location_data_assets import built_up_areas

from .utils.io_manager_helper.io_manager import (
    S3ParquetManager,
    S3ParquetManagerPartition,
    AwsWranglerDeltaLakeIOManager,
    S3JSONManager,
    PartitionedDuckDBParquetManager,
    PolarsDeltaLakeIOManager
)

from .jobs.analytics_platfom_jobs import (
    environment_job_1,
    environment_job_1_daily,
    environment_job_2,
    environment_job_2_monthly,
    trade_job_1,
    trade_job_1_daily,
    metadata_job_1,
    metadata_job_1_weekly,
    energy_job_1,
    energy_job_1_daily,
    infrastructure_job_1,
    infrastructure_job_1_weekly,
    infrastructure_job_2,
    infrastructure_job_2_daily,
    location_job_1
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
            london_datastore,
            ukpn_datastore_roadmap,
            analytics_platform_ea_flood_areas,
            analytics_platform_ea_flood_public_forecast,
            analytics_platform_dbt_trade_barrier_assets,
            analytics_platform_carbon_intensity_assets,
            national_charge_points_london_assets,
            entsog_uk_gas_assets,
            uk_power_networks_live_faults,
            national_charge_points_uk_assets,
            green_belt,
            built_up_areas,
            ukpn_smart_metres,
            renewable_energy_planning,
            uk_heat_networks
        ]
    ),
    jobs=[
        environment_job_1,
        environment_job_2,
        trade_job_1,
        metadata_job_1,
        energy_job_1,
        infrastructure_job_1,
        infrastructure_job_2,
        location_job_1
    ],
    schedules=[
        energy_job_1_daily,
        environment_job_1_daily,
        environment_job_2_monthly,
        trade_job_1_daily,
        infrastructure_job_1_weekly,
        metadata_job_1_weekly,
        infrastructure_job_2_daily
    ],
    sensors=[slack_failure_sensor],
    resources={
        "S3Parquet": S3ParquetManager(bucket_name=get_env_var("BRONZE_DATA_BUCKET")),
        "S3ParquetPartition": S3ParquetManagerPartition(bucket_name=get_env_var("BRONZE_DATA_BUCKET")),
        "S3Json": S3JSONManager(bucket_name=get_env_var("BRONZE_DATA_BUCKET")),
        "PartitionedDuckDBManager": PartitionedDuckDBParquetManager(bucket_name=get_env_var("BRONZE_DATA_BUCKET")),
        "DeltaLake": AwsWranglerDeltaLakeIOManager(bucket_name=get_env_var("SILVER_DATA_BUCKET")),
        "PolarsDeltaLake": PolarsDeltaLakeIOManager(bucket_name=get_env_var("SILVER_DATA_BUCKET")),
        "slack": SlackResource(token=get_env_var("SLACKBOT")),
    },
)
