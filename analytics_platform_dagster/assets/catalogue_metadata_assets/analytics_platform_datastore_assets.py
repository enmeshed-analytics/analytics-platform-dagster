import requests
import json
import pandas as pd

from dagster import asset, AssetIn, AssetExecutionContext
from ...models.catalogue_metadata_models.london_datastore import (
    LondonDatastoreCatalogue,
)


@asset(group_name="metadata_catalogues", io_manager_key="S3Json")
def london_datastore_bronze(context: AssetExecutionContext):
    """
    London Datastore Metadata bronze bucket
    """
    try:
        url = "https://data.london.gov.uk/api/datasets/export.json"
        response = requests.get(url)
        response.raise_for_status()
        data = json.loads(response.content)
        context.log.info(f"There were: {len(data)} catalogue items.")
        return data

    except Exception as e:
        raise e


@asset(
    group_name="metadata_catalogues",
    io_manager_key="DeltaLake",
    metadata={"mode": "overwrite"},
    ins={"london_datastore_bronze": AssetIn("london_datastore_bronze")},
)
def london_datastore_silver(context: AssetExecutionContext, london_datastore_bronze):
    """
    Process London Datastore Metadata into silver bucket.
    """

    # Make sure bronze bucket data can be read in
    input_data = london_datastore_bronze

    # Validate the data using the Pydantic model
    validated = LondonDatastoreCatalogue.model_validate({"items": input_data})

    # list to store data pre dataframe
    rows = []

    for item in validated.items:
        base_data = {
            "id": item.id,
            "title": item.title,
            "description": item.description,
            "author": item.author,
            "author_email": item.author_email,
            "maintainer": item.maintainer,
            "maintainer_email": item.maintainer_email,
            "licence": item.licence,
            "licence_notes": item.licence_notes,
            "update_frequency": item.update_frequency,
            "slug": item.slug,
            "state": item.state,
            "createdAt": item.createdAt,
            "updatedAt": item.updatedAt,
            "london_smallest_geography": item.london_smallest_geography,
            "tags": ", ".join(item.tags) if item.tags else "",
            "topics": ", ".join(item.topics) if item.topics else "",
            "shares": str(item.shares),
        }

        for resource_id, resource in item.resources.items():
            resource_data = {
                "resource_id": resource_id,
                "resource_title": resource.title,
                "resource_format": resource.format,
                "resource_url": resource.url,
                "resource_description": resource.description,
                "resource_check_hash": resource.check_hash,
                "resource_check_size": resource.check_size,
                "resource_check_timestamp": resource.check_timestamp,
            }
            rows.append({**base_data, **resource_data})

    df = pd.DataFrame(rows)

    context.log.info(f"Overview: {df.head(25)}")
    context.log.info(f"Overview 2: {df.columns}")
    context.log.info(f"Overview 2: {df.dtypes}")
    context.log.info(f"Overview 2: {df.shape}")

    df = df.astype(str)
    return df
