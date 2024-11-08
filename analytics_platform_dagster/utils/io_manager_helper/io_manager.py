import boto3
import json
import io
import awswrangler as wr
import pandas as pd
import polars as pl
import duckdb
import pyarrow as pa
import os

from datetime import datetime
from typing import List, Dict, Union, Any, Optional
from dagster import IOManager, OutputContext, InputContext
from botocore.exceptions import ClientError


# Errors for IO Managers
class InvalidDataTypeError(Exception):
    def __init__(self, message="Input must be a pandas DataFrame"):
        super().__init__(message)


class DeltaLakeWriteError(Exception):
    def __init__(self, message="Error writing to Delta Lake"):
        super().__init__(message)


class DeltaLakeReadError(Exception):
    def __init__(self, message="Error reading from Delta Lake"):
        super().__init__(message)


class S3BucketError(Exception):
    def __init__(self, message="Bucket can't be empty"):
        super().__init__(message)


class S3Error(Exception):
    def __init__(self, message="S3 Error") -> None:
        super().__init__(message)


# IO Managers
class AwsWranglerDeltaLakeIOManager(IOManager):
    """
    IO manager to handle reading and delta lake tables to S3.

    Uses AWSWrangler.

    Best used with aws-vault for credential access.
    """

    def __init__(self, bucket_name: str):
        if not bucket_name:
            raise S3BucketError()
        self.bucket_name = bucket_name

    def handle_output(self, context, obj):
        if not isinstance(obj, pd.DataFrame):
            raise InvalidDataTypeError()

        table_name = context.asset_key.path[-1]
        table_path = f"s3://{self.bucket_name}/{table_name}/"
        write_option = context.definition_metadata["mode"]

        try:
            wr.s3.to_deltalake(
                df=obj,
                path=table_path,
                mode=write_option, # mode (str, optional) – append (Default), overwrite, ignore, error
                schema_mode="overwrite",
                s3_allow_unsafe_rename=True,
            )
            context.log.info(f"Data written to Delta Lake table at {table_path}")
        except (DeltaLakeWriteError, Exception) as e:
            raise e

    def load_input(self, context):
        table_name = context.asset_key.path[-1]
        table_path = f"s3://{self.bucket_name}/{table_name}/"

        try:
            df = wr.s3.read_deltalake(table_path)
            return df
        except (DeltaLakeReadError, Exception) as e:
            raise e

class S3JSONManager(IOManager):
    """
    IO manager to handle reading and json files to S3.

    load_input method scans S3 bucket for latest file.

    """

    def __init__(self, bucket_name: str):
        if not bucket_name:
            raise S3BucketError()
        self.bucket_name = bucket_name
        self.aws_client = boto3.client("s3")

    def handle_output(self, context, obj):
        asset_name = context.asset_key.path[-1]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_key = f"{asset_name}_{timestamp}.json"
        try:
            json_str = json.dumps(obj)
            self.aws_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=json_str,
                ContentType="application/json",
            )
            context.log.info(
                f"JSON object written to S3 at s3://{self.bucket_name}/{object_key}"
            )
        except (S3Error, Exception) as e:
            raise e

    def load_input(self, context) -> List[Dict]:
        asset_name = context.asset_key.path[-1]
        prefix = f"{asset_name}_"
        try:
            # List objects in the bucket with the given prefix
            response = self.aws_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )

            # Check if any objects were found
            if "Contents" not in response or not response["Contents"]:
                raise FileNotFoundError(
                    f"No files found with prefix '{prefix}' in bucket '{self.bucket_name}'"
                )

            # Sort the objects by last modified date
            sorted_objects = sorted(
                response["Contents"], key=lambda x: x["LastModified"], reverse=True
            )

            # Get the latest object (first item after sorting)
            latest_object = sorted_objects[0]
            object_key = latest_object["Key"]

            # Get Json object from S3
            response = self.aws_client.get_object(
                Bucket=self.bucket_name, Key=object_key
            )
            json_str = response["Body"].read().decode("utf-8")
            json_data = json.loads(json_str)

            context.log.info(
                f"Loaded latest Json file: s3://{self.bucket_name}/{object_key}"
            )
            # Return Json object
            return json_data

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(
                    f"No files found with prefix '{prefix}' in bucket '{self.bucket_name}'"
                )
            else:
                raise e
        except Exception as e:
            raise e

class S3ParquetManager(IOManager):
    """
    IO manager to handle reading and parquet files to S3.

    load_input method scans S3 bucket for latest file.

    """

    def __init__(self, bucket_name: str):
        if not bucket_name:
            raise S3BucketError()
        self.bucket_name = bucket_name
        self.aws_client = boto3.client("s3")

    def handle_output(self, context, obj: pl.DataFrame):
        # Define context vars and object key
        asset_name = context.asset_key.path[-1]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_key = f"{asset_name}_{timestamp}.parquet"
        try:
            # Upload Parquet bytes to S3
            self.aws_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=obj,
                ContentType="application/octet-stream",
            )
            context.log.info(
                f"Parquet object written to S3 at s3://{self.bucket_name}/{object_key}"
            )
        except (S3Error, Exception) as e:
            raise e

    def load_input(self, context) -> pl.DataFrame:
        asset_name = context.asset_key.path[-1]
        prefix = f"{asset_name}_"
        try:
            # List objects in the bucket with the given prefix
            response = self.aws_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )

            # Check if any objects were found
            if "Contents" not in response or not response["Contents"]:
                raise FileNotFoundError(
                    f"No files found with prefix '{prefix}' in bucket '{self.bucket_name}'"
                )

            # Sort the objects by last modified date
            sorted_objects = sorted(
                response["Contents"], key=lambda x: x["LastModified"], reverse=True
            )

            # Get the latest object (first item after sorting
            # Fetch the key which will be the filename that you need to look for
            latest_object = sorted_objects[0]
            object_key = latest_object["Key"]

            # Get Parquet object from S3
            response = self.aws_client.get_object(
                Bucket=self.bucket_name, Key=object_key
            )
            parquet_bytes = response["Body"].read()

            # Convert Parquet bytes to DataFrame
            parquet_buffer = io.BytesIO(parquet_bytes)
            df = pl.read_parquet(parquet_buffer)

            context.log.info(
                f"Loaded latest Parquet file: s3://{self.bucket_name}/{object_key}"
            )
            # Return as a DataFrame
            return df

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(
                    f"No files found with prefix '{prefix}' in bucket '{self.bucket_name}'"
                )
            else:
                raise e
        except Exception as e:
            raise e

class PartitionedDuckDBParquetManager(IOManager):
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.con = duckdb.connect(':memory:')
        self._configure_duckdb()

    def _configure_duckdb(self):
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")
        self.con.execute("""
            CREATE SECRET (
                TYPE S3,
                PROVIDER CREDENTIAL_CHAIN
            );
        """)

    def _map_arrow_to_duckdb_type(self, arrow_type: Union[pa.DataType, pa.Field]) -> str:
        """
        Map Arrow types to DuckDB types, handling nested structures
        """
        # Handle list types
        if isinstance(arrow_type, pa.ListType):
            return 'JSON'

        # Handle struct types
        if isinstance(arrow_type, pa.StructType):
            return 'JSON'

        # Base type mapping
        base_types = {
            pa.string(): 'VARCHAR',
            pa.int64(): 'BIGINT',
            pa.int32(): 'INTEGER',
            pa.float64(): 'DOUBLE',
            pa.float32(): 'FLOAT',
            pa.bool_(): 'BOOLEAN',
            pa.date32(): 'DATE',
            pa.timestamp('us'): 'TIMESTAMP'
        }

        # Get the specific type instance for comparison
        for arrow_base_type, duckdb_type in base_types.items():
            if isinstance(arrow_type, type(arrow_base_type)):
                return duckdb_type

        # Default to VARCHAR for unknown types
        print(f"Unknown Arrow type {arrow_type}, defaulting to VARCHAR")
        return 'VARCHAR'


    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """
        Handle partitioned output to S3, creating a new partition for each batch
        """
        if not hasattr(obj, '__iter__'):
            raise TypeError("Expected an iterator or generator for batch processing")

        asset_name = context.asset_key.path[-1]
        base_path = f"s3://{self.bucket_name}/{asset_name}"
        temp_table = f"temp_{asset_name}"

        # Initialise on first batch
        first_batch = True
        batch_id = 0

        for batch_table in obj:  # batch_table is a PyArrow Table
            try:
                if first_batch:
                    # Log schema for debugging
                    context.log.debug(f"Processing first batch with schema: {batch_table.schema}")

                    # Create the temp table from first batch schema
                    columns = []
                    for field in batch_table.schema:
                        duckdb_type = self._map_arrow_to_duckdb_type(field)
                        columns.append(f"{field.name} {duckdb_type}")
                        context.log.debug(f"Mapped {field.name} ({field.type}) -> {duckdb_type}")

                    # Add metadata columns
                    columns.extend([
                        "batch_id INTEGER",
                        "process_timestamp TIMESTAMP"
                    ])

                    # Create the table
                    create_sql = f"""
                        CREATE TABLE IF NOT EXISTS {temp_table} (
                            {', '.join(columns)}
                        );
                    """
                    context.log.debug(f"Creating table with SQL: {create_sql}")
                    self.con.execute(create_sql)
                    first_batch = False

                # Register current batch
                self.con.register('current_batch', batch_table)

                # Insert with metadata
                insert_sql = f"""
                    INSERT INTO {temp_table}
                    SELECT
                        *,
                        {batch_id} as batch_id,
                        TIMESTAMP '{datetime.now()}' as process_timestamp
                    FROM current_batch;
                """
                self.con.execute(insert_sql)

                # Write partition
                self.con.execute(f"""
                    COPY (
                        SELECT * FROM {temp_table}
                        WHERE batch_id = {batch_id}
                    )
                    TO '{base_path}'
                    (
                        FORMAT PARQUET,
                        PARTITION_BY (batch_id),
                        OVERWRITE_OR_IGNORE
                    );
                """)

                # Cleanup
                self.con.execute(f"DELETE FROM {temp_table} WHERE batch_id = {batch_id};")
                self.con.execute("DROP VIEW IF EXISTS current_batch;")

                context.log.info(f"Successfully wrote batch {batch_id} to {base_path}/batch_id={batch_id}/")
                batch_id += 1

            except Exception as e:
                context.log.error(f"Error processing batch {batch_id}: {str(e)}")
                raise e

    def load_input(self, context: InputContext) -> pa.Table:
        """
        Load from partitioned S3 data, with optional batch filtering
        """
        asset_name = context.asset_key.path[-1]
        base_path = f"s3://{self.bucket_name}/{asset_name}"

        try:
            # Use glob pattern to read all partitions - TBC IF THIS WORKS THOUGH
            return self.con.execute(f"""
                SELECT * FROM read_parquet(
                    '{base_path}/*/*.parquet',
                    hive_partitioning=true
                )
                ORDER BY batch_id
            """).fetch_arrow_table()
        except Exception as e:
            context.log.error(f"Error loading data from {base_path}: {str(e)}")
            raise e

    def load_partition(self, context: InputContext, batch_id: int) -> pa.Table:
        """
        Load a specific partition by batch_id
        """
        asset_name = context.asset_key.path[-1]
        partition_path = f"s3://{self.bucket_name}/{asset_name}/batch_id={batch_id}"

        try:
            return self.con.execute(f"""
                SELECT * FROM read_parquet('{partition_path}/*.parquet')
            """).fetch_arrow_table()
        except Exception as e:
            context.log.error(f"Error loading partition {batch_id} from {partition_path}: {str(e)}")
            raise e

class PolarsDeltaLakeIOManager(IOManager):
    """
    IO manager to handle reading and writing delta lake tables to S3 using Polars.
    Supports AWS credentials from environment variables.
    """

    def __init__(
        self,
        bucket_name: str,
        region: str = "eu-west-2",
        storage_options: Optional[Dict[str, Any]] = None
    ):
        if not bucket_name:
            raise S3BucketError()

        self.bucket_name = bucket_name
        self.region = region

        # Default storage options
        self.storage_options = {
            "AWS_REGION": region,
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "AWS_SESSION_TOKEN": os.getenv("AWS_SESSION_TOKEN")
        }

        # Override defaults with provided storage options
        if storage_options:
            self.storage_options.update(storage_options)

    def handle_output(self, context: OutputContext, obj: pl.DataFrame) -> None:
        """Write Polars DataFrame to Delta Lake table in S3"""
        if not isinstance(obj, pl.DataFrame):
            raise InvalidDataTypeError()

        table_name = context.asset_key.path[-1]
        table_path = f"s3://{self.bucket_name}/{table_name}/"

        # Get write mode from metadata, default to overwrite
        write_option = context.definition_metadata["mode"]

        try:
            obj.write_delta(
                table_path,
                mode=write_option, # mode (str, optional) – append (Default), overwrite, ignore, error
                overwrite_schema=True,
                storage_options=self.storage_options
            )
            context.log.info(f"Successfully wrote data to Delta Lake table at {table_path}")

        except Exception as e:
            raise DeltaLakeWriteError(f"Failed to write to Delta Lake: {str(e)}")

    def load_input(self, context: InputContext) -> pl.DataFrame:
        """Read Delta Lake table from S3 into Polars DataFrame"""
        table_name = context.asset_key.path[-1]
        table_path = f"s3://{self.bucket_name}/{table_name}/"

        try:
            df = pl.read_delta(
                table_path,
                storage_options=self.storage_options
            )
            return df

        except Exception as e:
            raise DeltaLakeReadError(f"Failed to read from Delta Lake: {str(e)}")
