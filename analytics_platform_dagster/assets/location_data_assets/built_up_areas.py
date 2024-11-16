import os
import tempfile
import zipfile
import requests
import polars as pl
import fiona
import io

from shapely import wkt
from shapely.geometry import shape
from dagster import AssetExecutionContext, Output, asset
from ...utils.variables_helper.url_links import asset_urls
from ...utils.requests_helper.requests_helper import fetch_redirect_url

@asset(
    group_name="location_assets",
    io_manager_key="S3ParquetPartition",
)
def os_built_up_areas_bronze(context: AssetExecutionContext):
    """
    Processes OS Built Up Areas data and returns a generator of parquet bytes for S3 storage

    Flow:
    ZIP → GPKG → Batch → DataFrame → Parquet Bytes → S3

    Directory structure in S3:
    bucket/
      └── asset_name/
          ├── batch0001_20240116.parquet
          ├── batch0002_20240116.parquet
          └── batchNumber_20240116.parquet
    """
    def generate_batches():
        BATCH_SIZE = 2000
        batch_number = 0
        errors = []
        current_batch = []

        # Fetch and validate URL
        url = asset_urls.get("os_built_up_areas")
        if url is None:
            raise ValueError("No URL provided")

        redirect_url = fetch_redirect_url(url)
        if redirect_url is None:
            raise ValueError("No redirect URL found")

        # Create temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download and extract zip
            zip_path = os.path.join(temp_dir, 'temp.zip')
            response = requests.get(redirect_url)
            response.raise_for_status()

            with open(zip_path, 'wb') as zip_file:
                zip_file.write(response.content)

            # Extract GPKG file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # Find GPKG file
            gpkg_file = next(
                (os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('gpkg')),
                None
            )
            if not gpkg_file:
                raise FileNotFoundError("No GPKG file found in zip")

            # Process GPKG file
            with fiona.open(gpkg_file, 'r') as src:
                context.log.info(f"CRS: {src.crs}")
                context.log.info(f"Schema: {src.schema}")

                for i, feature in enumerate(src):
                    try:
                        # Extract properties and add geometry as WKT
                        properties = dict(feature['properties'])
                        geom = shape(feature['geometry'])
                        properties['geometry'] = wkt.dumps(geom)

                        # Ensure all required fields are present with correct types
                        processed_properties = {
                            'gsscode': str(properties.get('gsscode', '')),
                            'name1_text': str(properties.get('name1_text', '')),
                            'name1_language': str(properties.get('name1_language', '')),
                            'name2_text': str(properties.get('name2_text', '')),
                            'name2_language': str(properties.get('name2_language', '')),
                            'areahectares': float(properties.get('areahectares', 0.0)),
                            'geometry_area_m': float(properties.get('geometry_area_m', 0.0)),
                            'geometry': properties['geometry']
                        }

                        current_batch.append(processed_properties)

                    # Hanlde errors if there are any and log them
                    except Exception as e:
                        error_msg = f"Error processing feature {i}: {e}"
                        context.log.error(error_msg)
                        errors.append(error_msg)
                        continue

                    # When the batch size is reached, convert to parquet bytes and yield
                    if len(current_batch) >= BATCH_SIZE:
                        batch_number += 1

                        # Convert batch to DataFrame then to parquet bytes
                        df_batch = pl.DataFrame(current_batch)
                        context.log.info(f"Processed final batch {df_batch.head(25)}")
                        parquet_buffer = io.BytesIO()
                        df_batch.write_parquet(parquet_buffer)
                        parquet_bytes = parquet_buffer.getvalue()

                        yield parquet_bytes

                        # Reset for next batch
                        current_batch = []
                        errors = []
                        context.log.info(f"Processed batch {batch_number}")

                # Process final batch if any remains
                if current_batch:
                    batch_number += 1
                    df_batch = pl.DataFrame(current_batch)
                    context.log.info(f"Processed final batch {df_batch.head(25)}")
                    parquet_buffer = io.BytesIO()
                    df_batch.write_parquet(parquet_buffer)
                    parquet_bytes = parquet_buffer.getvalue()
                    yield parquet_bytes
                    context.log.info(f"Processed final batch {batch_number}")

    return Output(
        value=generate_batches(),
        metadata={
            "description": "Generator of parquet bytes for batch processing"
        }
    )
