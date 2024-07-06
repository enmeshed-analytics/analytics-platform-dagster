from dagster import Definitions
from .jobs.analytics_platfom_jobs import ea_job_1, dbt_job_1, os_job_1
from .assets.analytics_platform_ea_assets import ea_floods, ea_flood_areas
from .assets.analytics_platform_dbt_assets import dbt_trade_barriers
from .assets.analytics_platform_os_assets import os_open_usrns

# Define jobs for different orgs and agencies
env_agency = Definitions(
    assets=[ea_flood_areas, ea_floods,],
    jobs=[ea_job_1]
)

dept_bus_trade = Definitions(
    assets=[dbt_trade_barriers],
    jobs=[dbt_job_1]
)

ordanance_survey = Definitions(
    assets=[os_open_usrns],
    jobs=[os_job_1]
)
