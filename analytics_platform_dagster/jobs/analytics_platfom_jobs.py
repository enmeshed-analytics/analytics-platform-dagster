from dagster import define_asset_job, ScheduleDefinition

# Evironment
environment_job_1 = define_asset_job(
    name="environment_job_1", selection=["ea_flood_areas_bronze", "ea_flood_areas_silver"]
)

# Trade
trade_job_1 = define_asset_job(
    name="trade_job_1",
    selection=["dbt_trade_barriers_bronze", "dbt_trade_barriers_silver"],
)

trade_job_1_daily = ScheduleDefinition(
    job=trade_job_1,
    cron_schedule="0 1 * * *",
    execution_timezone="Europe/London",
    name="trade_daily_schedule",
)

# Metadata
metadata_job_1 = define_asset_job(
    name="metadata_job_1",
    selection=["london_datastore_bronze", "london_datastore_silver"],
)

# Energy
energy_job_1 = define_asset_job(
    name="energy_job_1",
    selection=[
        "carbon_intensity_bronze",
        "carbon_intensity_silver",
        "entsog_gas_uk_data_bronze",
        "entsog_gas_uk_data_silver",
    ],
)

energy_job_1_daily = ScheduleDefinition(
    job=energy_job_1,
    cron_schedule="0 0 * * *",
    execution_timezone="Europe/London",
    name="energy_daily_schedule",
)

# Infrastructure
infrastructure_job_1 = define_asset_job(
    name="infrastructure_job_1",
    selection=[
        "national_charge_point_data_bronze",
        "national_charge_point_data_silver",
    ],
)
