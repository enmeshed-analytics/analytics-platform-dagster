from typing import Dict, List, Optional
from dagster import asset, AssetIn, AssetExecutionContext
import polars as pl

from ...models.catalogue_metadata_models.london_datastore import LondonDatastoreCatalogue
from ...utils.slack_messages.slack_message import with_slack_notification
from ...utils.variables_helper.url_links import asset_urls
from ...utils.etl_patterns.bronze_factory import bronze_asset_factory

def transform_london_datastore(data: List[Dict]) -> List[Dict]:
    """Transform London Datastore data into flattened format"""
    rows = []
    for item in data:
        base_data = {
            "id": str(item.get("id", "")),
            "title": str(item.get("title", "")),
            "description": str(item.get("description", "")),
            "author": str(item.get("author", "")),
            "author_email": str(item.get("author_email", "")),
            "maintainer": str(item.get("maintainer", "")),
            "maintainer_email": str(item.get("maintainer_email", "")),
            "licence": str(item.get("licence", "")),
            "licence_notes": str(item.get("licence_notes", "")),
            "update_frequency": str(item.get("update_frequency", "")),
            "slug": str(item.get("slug", "")),
            "state": str(item.get("state", "")),
            "createdAt": str(item.get("createdAt", "")),
            "updatedAt": str(item.get("updatedAt", "")),
            "london_smallest_geography": str(item.get("london_smallest_geography", "")),
            "tags": ", ".join(item.get("tags", [])) if item.get("tags") else "",
            "topics": ", ".join(item.get("topics", [])) if item.get("topics") else "",
            "shares": str(item.get("shares", "")),
        }

        resources = item.get("resources", {})
        if resources:
            for resource_id, resource in resources.items():
                resource_data = {
                    "resource_id": str(resource_id),
                    "resource_title": str(resource.get("title", "")),
                    "resource_format": str(resource.get("format", "")),
                    "resource_url": str(resource.get("url", "")),
                    "resource_description": str(resource.get("description", "")),
                    "resource_check_hash": str(resource.get("check_hash", "")),
                    "resource_check_size": str(resource.get("check_size", "")),
                    "resource_check_timestamp": str(resource.get("check_timestamp", "")),
                }
                rows.append({**base_data, **resource_data})
        else:
            rows.append(base_data)
    return rows

@asset(
    name="london_datastore_bronze",
    group_name="metadata_catalogues",
    io_manager_key="S3Parquet"
)
@bronze_asset_factory(
    url_key="london_data_store",
    model=LondonDatastoreCatalogue,
    asset_urls=asset_urls,
    transform_func=transform_london_datastore,
    wrap_items=True
)
def london_datastore_bronze(context: AssetExecutionContext) -> Optional[bytes]:
    """Load London Datastore Metadata as Parquet to bronze bucket"""
    return None

@asset(
    name="london_datastore_silver",
    group_name="metadata_catalogues",
    io_manager_key="PolarsDeltaLake",
    metadata={"mode": "overwrite"},
    ins={
        "london_datastore_bronze": AssetIn("london_datastore_bronze")
    },
    required_resource_keys={"slack"}
)
@with_slack_notification("London Datastore Catalogue Data")
def london_datastore_silver(
    context: AssetExecutionContext,
    london_datastore_bronze: pl.DataFrame
) -> pl.DataFrame:
    """Process London Datastore Metadata into silver bucket"""
    df = london_datastore_bronze

    df = df.with_columns([
        pl.when(pl.col("createdAt").str.contains(r"\."))
        .then(pl.col("createdAt").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.fZ", strict=False))
        .otherwise(pl.col("createdAt").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ", strict=False))
        .cast(pl.Date),

        pl.when(pl.col("updatedAt").str.contains(r"\."))
        .then(pl.col("updatedAt").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.fZ", strict=False))
        .otherwise(pl.col("updatedAt").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ", strict=False))
        .cast(pl.Date)
    ])

    context.log.info(f"Overview: {df.head(25)}")
    context.log.info(f"Columns: {df.columns}")
    context.log.info(f"Types: {df.dtypes}")
    context.log.info(f"Shape: {df.shape}")

    return df
