from typing import Optional
from dagster import asset, AssetExecutionContext

from ...utils.variables_helper.url_links import asset_urls
from ...utils.etl_patterns.bronze_factory import bronze_asset_factory

@asset(
    name="ukpn_smart_metres",
    group_name="energy_assets",
    io_manager_key="S3Parquet"
)
@bronze_asset_factory(
    url_key="ukpn_smart_metres",
    asset_urls=asset_urls,
)
def ukpn_smart_metres(context: AssetExecutionContext) -> Optional[bytes]:
    """Load UKPN Smart Metres as Parquet to bronze bucket"""
    return None
