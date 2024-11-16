from dagster import define_asset_job, ScheduleDefinition

# ENVIRONMENT
environment_job_1 = define_asset_job(
    name="environment_job_1",
    selection=[
        "ea_flood_areas_bronze",
        "ea_flood_areas_silver",
        "ea_flood_public_forecast_bronze",
        "ea_flood_public_forecast_silver",
    ]
)

environment_job_1_daily = ScheduleDefinition(
    job=environment_job_1,
    cron_schedule="0 2 * * *",
    execution_timezone="Europe/London",
    name="environment_daily_schedule",
)

environment_job_2 = define_asset_job(
    name="environment_job_2",
    selection=[
        "green_belt_bronze",
        "green_belt_silver",
    ]
)
environment_job_2_monthly = ScheduleDefinition(
    job=environment_job_2,
    cron_schedule="0 5 1 * *",
    execution_timezone="Europe/London",
    name="environment_monthly_schedule",
)

# TRADE
trade_job_1 = define_asset_job(
    name="trade_job_1",
    selection=[
        "dbt_trade_barriers_bronze",
        "dbt_trade_barriers_silver"
    ]
)

trade_job_1_daily = ScheduleDefinition(
    job=trade_job_1,
    cron_schedule="0 1 * * *",
    execution_timezone="Europe/London",
    name="trade_daily_schedule",
)

# METADATA
metadata_job_1 = define_asset_job(
    name="metadata_job_1",
    selection=[
        "london_datastore_bronze",
        "london_datastore_silver"
    ]
)

metadata_job_1_weekly = ScheduleDefinition(
    job=metadata_job_1,
    cron_schedule="0 3 * * 1",
    execution_timezone="Europe/London",
    name="metatdata_weekly_schedule",
)

# ENERGY
energy_job_1 = define_asset_job(
    name="energy_job_1",
    selection=[
        "carbon_intensity_bronze",
        "carbon_intensity_silver",
        "entsog_gas_uk_data_bronze",
        "entsog_gas_uk_data_silver",
    ]
)

energy_job_1_daily = ScheduleDefinition(
    job=energy_job_1,
    cron_schedule="0 0 * * *",
    execution_timezone="Europe/London",
    name="energy_daily_schedule",
)

# INFRASTRUCTURE
infrastructure_job_1 = define_asset_job(
    name="infrastructure_job_1",
    selection=[
        "national_charge_point_london_bronze",
        "national_charge_point_london_silver",
        "national_charge_point_uk_bronze",
        "national_charge_point_uk_silver",
    ]
)

infrastructure_job_1_weekly = ScheduleDefinition(
    job=infrastructure_job_1,
    cron_schedule="0 4 * * 1",
    execution_timezone="Europe/London",
    name="infrastructure_weekly_schedule",
)

infrastructure_job_2 = define_asset_job(
    name="infrastructure_job_2",
    selection=[
        "ukpn_live_faults_bronze",
        "ukpn_live_faults_silver",
    ]
)

infrastructure_job_2_daily = ScheduleDefinition(
    job=infrastructure_job_2,
    cron_schedule="0 7 * * *",
    execution_timezone="Europe/London",
    name="infrastructure_daily_schedule",
)

# LOCATION
location_job_1 = define_asset_job(
    name="location_job_1",
    selection=[
        "os_built_up_areas_bronze"
    ]
)
