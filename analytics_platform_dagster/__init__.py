from dagster import Definitions

from .jobs.analytics_platfom_job_1 import assets_job_1
from .assets.analytics_platform_assets_1 import dbt_trade_barriers, ea_flood_areas, ea_floods, os_open_usrns

defs = Definitions(
    assets=[dbt_trade_barriers, ea_flood_areas, ea_floods, os_open_usrns],
    jobs=[assets_job_1]
)