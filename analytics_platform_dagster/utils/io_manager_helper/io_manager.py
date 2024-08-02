import boto3
import json
import io
import awswrangler as wr
import pandas as pd

from datetime import datetime
from typing import List, Dict
from dagster import IOManager
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
    
    Best used with aws-vault for credentails access.
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
                mode=write_option,
                schema_mode="overwrite", # INCLUDE AS A CONTEXT ARG AT SOME POINT LIKE FOR MODE ABOVE
                s3_allow_unsafe_rename=True
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
        self.aws_client = boto3.client('s3')

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
                ContentType='application/json'
            )
            context.log.info(f"JSON object written to S3 at s3://{self.bucket_name}/{object_key}")
        except (S3Error, Exception) as e:
            raise e

    def load_input(self, context) -> List[Dict]:
        asset_name = context.asset_key.path[-1]
        prefix = f"{asset_name}_"
        try:
            # List objects in the bucket with the given prefix
            response = self.aws_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            # Check if any objects were found
            if 'Contents' not in response or not response['Contents']:
                raise FileNotFoundError(f"No files found with prefix '{prefix}' in bucket '{self.bucket_name}'")

            # Sort the objects by last modified date
            sorted_objects = sorted(
                response['Contents'],
                key=lambda x: x['LastModified'],
                reverse=True
            )

            # Get the latest object (first item after sorting)
            latest_object = sorted_objects[0]
            object_key = latest_object['Key']

            # Get Json object from S3            
            response = self.aws_client.get_object(Bucket=self.bucket_name, Key=object_key)
            json_str = response['Body'].read().decode('utf-8')
            json_data = json.loads(json_str)

            context.log.info(f"Loaded latest Json file: s3://{self.bucket_name}/{object_key}")
            # Return Json object
            return json_data

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"No files found with prefix '{prefix}' in bucket '{self.bucket_name}'")
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
        self.aws_client = boto3.client('s3')

    def handle_output(self, context, object: pd.DataFrame):
        # Define context vars and object key
        asset_name = context.asset_key.path[-1]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_key = f"{asset_name}_{timestamp}.parquet"
        try:
            # Upload Parquet bytes to S3
            self.aws_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=object,
                ContentType='application/octet-stream'
            )
            context.log.info(f"Parquet object written to S3 at s3://{self.bucket_name}/{object_key}")
        except (S3Error, Exception) as e:
            raise e

    def load_input(self, context) -> pd.DataFrame:
        asset_name = context.asset_key.path[-1]
        prefix = f"{asset_name}_"
        try:
            # List objects in the bucket with the given prefix
            response = self.aws_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            # Check if any objects were found
            if 'Contents' not in response or not response['Contents']:
                raise FileNotFoundError(f"No files found with prefix '{prefix}' in bucket '{self.bucket_name}'")

            # Sort the objects by last modified date
            sorted_objects = sorted(
                response['Contents'],
                key=lambda x: x['LastModified'],
                reverse=True
            )

            # Get the latest object (first item after sorting
            # Fetch the key which will be the filename that you need to look for
            latest_object = sorted_objects[0]
            object_key = latest_object['Key']

            # Get Parquet object from S3
            response = self.aws_client.get_object(Bucket=self.bucket_name, Key=object_key)
            parquet_bytes = response['Body'].read()

            # Convert Parquet bytes to DataFrame
            parquet_buffer = io.BytesIO(parquet_bytes)
            df = pd.read_parquet(parquet_buffer)

            context.log.info(f"Loaded latest Parquet file: s3://{self.bucket_name}/{object_key}")
            # Return as a DataFrame
            return df

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"No files found with prefix '{prefix}' in bucket '{self.bucket_name}'")
            else:
                raise e
        except Exception as e:
            raise e
