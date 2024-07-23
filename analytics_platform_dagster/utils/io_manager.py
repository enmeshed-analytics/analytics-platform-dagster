from dagster import IOManager
import awswrangler as wr
import pandas as pd

# Errors for IO Manager
class InvalidDataTypeError(Exception):
    def __init__(self, message="Input must be a pandas DataFrame"):
        self.message = message
        super().__init__(self.message)

class BucketNameError(Exception):
    def __init__(self, message="Invalid bucket name"):
        self.message = message
        super().__init__(self.message)

class DeltaLakeWriteError(Exception):
    def __init__(self, message="Error writing to Delta Lake"):
        self.message = message
        super().__init__(self.message)

class DeltaLakeReadError(Exception):
    def __init__(self, message="Error reading from Delta Lake"):
        self.message = message
        super().__init__(self.message)

# IO Manager
class AwsWranglerDeltaLakeIOManager(IOManager):
    def __init__(self, bucket_name: str):
        if not bucket_name:
            raise BucketNameError("Bucket name cannot be empty")
        self.bucket_name = bucket_name

    def handle_output(self, context, obj):
        if not isinstance(obj, pd.DataFrame):
            raise InvalidDataTypeError()

        table_name = context.asset_key.path[-1]
        table_path = f"s3://{self.bucket_name}/{table_name}/"

        try:
            wr.s3.to_deltalake(
                df=obj,
                path=table_path,
                mode="overwrite",
                s3_allow_unsafe_rename=True
            )
            context.log.info(f"Data written to Delta Lake table at {table_path}")
        except Exception as e:
            raise DeltaLakeWriteError(f"Failed to write to Delta Lake: {str(e)}")

    def load_input(self, context):
        table_name = context.asset_key.path[-1]
        table_path = f"s3://{self.bucket_name}/{table_name}/"

        try:
            df = wr.s3.read_deltalake(table_path)
            return df
        except Exception as e:
            raise DeltaLakeReadError(f"Failed to read from Delta Lake: {str(e)}")