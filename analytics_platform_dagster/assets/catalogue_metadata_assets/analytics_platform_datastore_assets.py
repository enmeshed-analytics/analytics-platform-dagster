import requests
import json
import polars as pl
import io

from pydantic import ValidationError
from dagster import asset, AssetIn, AssetExecutionContext
from ...models.catalogue_metadata_models.london_datastore import (
    LondonDatastoreCatalogue,
)
from ...utils.slack_messages.slack_message import with_slack_notification
from ...utils.variables_helper.url_links import asset_urls

@asset(group_name="metadata_catalogues", io_manager_key="S3Parquet")
def london_datastore_bronze(context: AssetExecutionContext):
    """
    Load London Datastore Metadata as Parquet to bronze bucket
    """
    try:
        url = asset_urls.get("london_data_store")
        if url is None:
            raise ValueError("URL for london_data_store is not found in asset_urls")

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        validation_errors = []
        try:
            validated = LondonDatastoreCatalogue.model_validate({"items": data})
        except ValidationError as e:
            validation_errors = e.errors()

        # Create flattened rows
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

            # Handle resources
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
                # If no resources, still include the base data
                rows.append(base_data)

        # Create DataFrame from flattened data
        df = pl.DataFrame(rows)

        context.log.info(f"Processed {len(df)} records with {len(validation_errors)} validation errors")

        parquet_buffer = io.BytesIO()
        df.write_parquet(parquet_buffer)
        parquet_bytes = parquet_buffer.getvalue()

        context.log.info("Successfully processed Parquet data to S3")
        return parquet_bytes

    except Exception as e:
        raise e


@asset(
    group_name="metadata_catalogues",
    io_manager_key="PolarsDeltaLake",
    metadata={"mode": "overwrite"},
    ins={"london_datastore_bronze": AssetIn("london_datastore_bronze")},
    required_resource_keys={"slack"}
)
@with_slack_notification("London Datastore Catalogue Data")
def london_datastore_silver(context: AssetExecutionContext, london_datastore_bronze):
    """
    Process London Datastore Metadata into silver bucket.
    """

    df = pl.DataFrame(london_datastore_bronze)

    df = df.with_columns([
        pl.when(pl.col("createdAt").str.contains(r"\."))
        .then(pl.col("createdAt").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S.%fZ", strict=False))
        .otherwise(pl.col("createdAt").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ", strict=False))
        .cast(pl.Date),

        pl.when(pl.col("updatedAt").str.contains(r"\."))
        .then(pl.col("updatedAt").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S.%fZ", strict=False))
        .otherwise(pl.col("updatedAt").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ", strict=False))
        .cast(pl.Date)
    ])

    context.log.info(f"Overview: {df.head(25)}")
    context.log.info(f"Overview 2: {df.columns}")
    context.log.info(f"Overview 2: {df.dtypes}")
    context.log.info(f"Overview 2: {df.shape}")

    return df
