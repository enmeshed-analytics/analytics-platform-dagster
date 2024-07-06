from dagster import define_asset_job

ea_job_1 = define_asset_job(
    name="analytics_platform_ea_assets",
    selection=["ea_flood_areas", "ea_floods"]
)

dbt_job_1 = define_asset_job(
    name="analytics_platform_dbt_assets",
    selection=["dbt_trade_barriers"]
)

os_job_1 = define_asset_job(
    name="analytics_platform_os_assets",
    selection=["os_open_usrns"]
)
