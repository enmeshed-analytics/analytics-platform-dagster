from typing import Optional
from dagster import asset, AssetExecutionContext

from ...utils.variables_helper.url_links import asset_urls
from ...utils.etl_patterns.bronze_factory import bronze_asset_factory

@asset(
    name="uk_heat_networks_bronze",
    group_name="infrastructure_assets",
    io_manager_key="S3Parquet"
)
@bronze_asset_factory(
    url_key="uk_heat_networks",
    asset_urls=asset_urls,
    sheet_name="Heat Networks"
)
def uk_heat_networks_bronze(context: AssetExecutionContext) -> Optional[bytes]:
    """Load UK Heat Network Dataset as Parquet to bronze bucket"""
    return None
