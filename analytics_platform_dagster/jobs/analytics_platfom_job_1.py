from dagster import define_asset_job

assets_job_1 = define_asset_job(
    name="analytics_platform_ingest_1",
    selection=["dbt_trade_barriers", "ea_flood_areas", "ea_floods", "os_open_usrns"],
)