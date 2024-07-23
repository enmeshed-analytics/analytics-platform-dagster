from dagster import define_asset_job

ea_job_1 = define_asset_job(
    name="analytics_platform_ea_assets",
    selection=["ea_floods", "ea_flood_areas"]
)

dbt_job_1 = define_asset_job(
    name="analytics_platform_dbt_assets",
    selection=["dbt_trade_barriers", "dbt_trade_barriers_transform", "dbt_trade_barriers_delta_lake"]
)

os_job_1 = define_asset_job(
    name="analytics_platform_os_assets",
    selection=["os_open_usrns"]
)

datastore_job_1 = define_asset_job(
    name="analytics_platform_datastore_assets",
    selection=["london_datastore"]
)
