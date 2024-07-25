from dagster import define_asset_job

environment_job_1 = define_asset_job(
    name="analytics_platform_ea_assets",
    selection=["ea_floods", "ea_flood_areas"]
)

trade_job_1 = define_asset_job(
    name="analytics_platform_dbt_assets",
    selection=["dbt_trade_barriers", "dbt_trade_barriers_transform", "dbt_trade_barriers_delta_lake"]
)

metadata_job_1 = define_asset_job(
    name="analytics_platform_datastore_assets",
    selection=["london_datastore"]
)

energy_job_1 = define_asset_job(
    name="analytics_platform_carbon_intensity_assets",
    selection=["fetch_carbon_data", "carbon_intensity_delta_lake"]
)