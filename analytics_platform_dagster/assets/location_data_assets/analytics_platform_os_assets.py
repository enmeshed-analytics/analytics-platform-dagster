"""
Need to sort this for delta lake compatability
and figure out if it's worth having the whole dataset given how big it is...
could use spark for this one?
"""

#import requests
# import duckdb
# import pandas as pd
# import zipfile
# import tempfile
# import os
# import time
# import fiona

# from ...utils.duckdb_helper.duckdb_helper_open_usrns import process_chunk_os_usrn
# from shapely import wkt
# from shapely.geometry import shape
# from loguru import logger
# from dagster import asset, OpExecutionContext

# def wait_10_seconds(context: OpExecutionContext):
#     time.sleep(10)
#     context.log.info("Waited for 10 seconds")

# @asset
# def os_open_usrns(context: OpExecutionContext):
#     """
#     Download, process, and load OS Open USRN (Unique Street Reference Number) data into a DuckDB database.

#         This asset performs the following steps:
#         1. Retrieves the redirect URL for the OS Open USRN GeoPackage.
#         2. Downloads the zipped GeoPackage file.
#         3. Extracts the GeoPackage from the zip file.
#         4. Processes the GeoPackage data in batches.
#         5. Loads the processed data into a DuckDB table named 'open_usrns_table'.

#         The asset handles large datasets efficiently by processing the data in chunks
#         and converts geometries to Well-Known Text (WKT) format for storage.

#         Args:
#             context (OpExecutionContext): The execution context provided by Dagster.

#         Returns:
#             None

#         Raises:
#             FileNotFoundError: If no GeoPackage file is found in the downloaded zip archive.
#             Exception: For any errors during the download, extraction, or processing steps.
#     """

#     db_file = 'data.duckdb'
#     # Could make the batch size larger
#     batch_size = 50000
#     url = "https://api.os.uk/downloads/v1/products/OpenUSRN/downloads?area=GB&format=GeoPackage&redirect"

#     try:
#         # Step 1: Retrieve the redirect URL
#         response = requests.get(url)
#         response.raise_for_status()
#         redirect_url = response.url
#         logger.success(f"Success! Redirect URL is: {redirect_url}")

#         # Step 2: Download the content from the redirect URL
#         response = requests.get(redirect_url)
#         response.raise_for_status()
#         zip_content = response.content
#         logger.success("Zip file downloaded")

#         # Step 3: Create a temporary directory
#         with tempfile.TemporaryDirectory() as temp_dir:
#             # Step 4: Write the zip file to the temporary directory
#             zip_path = os.path.join(temp_dir, 'temp.zip')
#             with open(zip_path, 'wb') as zip_file:
#                 zip_file.write(zip_content)

#             # Step 5: Extract the contents of the zip file
#             with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#                 zip_ref.extractall(temp_dir)

#             # Step 6: Find the GeoPackage file
#             gpkg_file = None
#             for file_name in os.listdir(temp_dir):
#                 if file_name.endswith('.gpkg'):
#                     gpkg_file = os.path.join(temp_dir, file_name)
#                     break

#             if gpkg_file:
#                 try:
#                     # Create a DuckDB connection
#                     with duckdb.connect(database=db_file, read_only=False) as conn:
#                         # Create the table if it doesn't exist
#                         conn.execute("""
#                             CREATE TABLE OR REPLACE open_usrns_table (
#                                 geometry VARCHAR,
#                                 street_type VARCHAR,
#                                 usrn INTEGER
#                             )
#                         """)
#                         # Process the GeoPackage in chunks
#                         with fiona.open(gpkg_file, 'r') as src:
#                             features = []
#                             for i, feature in enumerate(src):
#                                 try:
#                                     # Convert geometry to WKT string
#                                     geom = shape(feature['geometry'])
#                                     feature['properties']['geometry'] = wkt.dumps(geom)
#                                 except Exception as e:
#                                     feature['properties']['geometry'] = None
#                                     logger.warning(f"Error converting geometry for feature {i}: {e}")
#                                 features.append(feature['properties'])
#                                 if len(features) == batch_size:
#                                     # Process the chunk
#                                     df_chunk = pd.DataFrame(features)
#                                     process_chunk_os_usrn(df_chunk, conn)
#                                     logger.success(f"Processed features {i-batch_size+1} to {i}")
#                                     features = []
#                             # Process any remaining features
#                             if features:
#                                 df_chunk = pd.DataFrame(features)
#                                 process_chunk_os_usrn(df_chunk, conn)
#                                 logger.success("Processed final chunk")
#                     logger.success("Data loaded into DuckDB successfully")
#                 except Exception as e:
#                     logger.error(f"Error processing GeoPackage: {e}")
#                     raise
#             else:
#                 raise FileNotFoundError("No GeoPackage file found in the zip archive")
#     except Exception as e:
#         logger.error(f"An error ocurred: {e}")
#         raise
#     wait_10_seconds(context)
