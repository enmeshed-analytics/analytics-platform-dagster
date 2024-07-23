from dagster import Definitions, load_assets_from_modules
from .assets import analytics_platform_datastore_assets, analytics_platform_dbt_assets, analytics_platform_ea_assets, analytics_platform_os_assets
from .jobs.analytics_platfom_jobs import ea_job_1, dbt_job_1, os_job_1, datastore_job_1

defs = Definitions(
    assets=load_assets_from_modules([analytics_platform_datastore_assets, analytics_platform_os_assets, analytics_platform_ea_assets, analytics_platform_dbt_assets]),
    jobs=[ea_job_1, dbt_job_1, os_job_1, datastore_job_1]
)
