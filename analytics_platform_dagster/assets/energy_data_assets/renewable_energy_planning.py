from typing import Optional
from dagster import asset, AssetExecutionContext

from ...utils.variables_helper.url_links import asset_urls
from ...utils.etl_patterns.bronze_factory import bronze_asset_factory

@asset(
    name="renewable_energy_planning",
    group_name="energy_assets",
    io_manager_key="S3Parquet"
)
@bronze_asset_factory(
    url_key="renewable_energy_planning",
    asset_urls=asset_urls,
    sheet_name="REPD"
)
def renewable_energy_planning_bronze(context: AssetExecutionContext) -> Optional[bytes]:
    """Load Renewable Energy Planing Dataset as Parquet to bronze bucket"""
    return None
