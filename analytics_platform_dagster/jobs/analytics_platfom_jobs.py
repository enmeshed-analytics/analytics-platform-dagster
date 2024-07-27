from dagster import define_asset_job, ScheduleDefinition

# Evironment
environment_job_1 = define_asset_job(
    name="analytics_platform_ea_assets",
    selection=["ea_floods", "ea_flood_areas"]
)

# Trade
trade_job_1 = define_asset_job(
    name="analytics_platform_dbt_assets",
    selection=["dbt_trade_barriers", "dbt_trade_barriers_transform", "dbt_trade_barriers_delta_lake"]
)

# Metadata
metadata_job_1 = define_asset_job(
    name="analytics_platform_datastore_assets",
    selection=["london_datastore"]
)

# Energy
energy_job_1 = define_asset_job(
    name="analytics_platform_carbon_intensity_assets",
    selection=["carbon_intensity_bronze", "carbon_intensity_silver"]
)

energy_job_1_daily = ScheduleDefinition(
    job=energy_job_1,
    cron_schedule="0 0 * * *",  
    execution_timezone="Europe/London",
    name="energy_daily_schedule"
)
