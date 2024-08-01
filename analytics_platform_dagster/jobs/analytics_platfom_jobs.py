from dagster import define_asset_job, ScheduleDefinition

# Evironment
environment_job_1 = define_asset_job(
    name="environment_job_1",
    selection=["ea_floods", "ea_flood_areas"]
)

# Trade
trade_job_1 = define_asset_job(
    name="trade_job_1",
    selection=["dbt_trade_barriers_bronze"]
)

# Metadata
metadata_job_1 = define_asset_job(
    name="metadata_job_1",
    selection=["london_datastore"]
)

# Energy
energy_job_1 = define_asset_job(
    name="energy_job_1",
    selection=["carbon_intensity_bronze", "carbon_intensity_silver"]
)

energy_job_1_daily = ScheduleDefinition(
    job=energy_job_1,
    cron_schedule="0 0 * * *",  
    execution_timezone="Europe/London",
    name="energy_daily_schedule"
)

# Infrastructure
infrastructure_job_1 = define_asset_job(
    name="infrastructure_job_1",
    selection=["national_charge_point_data_bronze", "national_charge_point_data_silver"]
)