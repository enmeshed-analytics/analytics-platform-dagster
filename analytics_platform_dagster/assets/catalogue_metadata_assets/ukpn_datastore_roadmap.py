from typing import Optional
from dagster import asset, AssetExecutionContext

from ...utils.variables_helper.url_links import asset_urls
from ...utils.etl_patterns.bronze_factory import bronze_asset_factory

@asset(
    name="ukpn_datastore_roadmap_bronze",
    group_name="metadata_catalogues",
    io_manager_key="S3Parquet"
)
@bronze_asset_factory(
    url_key="ukpn_datastore_roadmap_bronze",
    asset_urls=asset_urls,
)
def ukpn_datastore_roadmap_bronze(context: AssetExecutionContext) -> Optional[bytes]:
    """Load UKPN Datastore Roadmap Metadata as Parquet to bronze bucket"""
    return None
