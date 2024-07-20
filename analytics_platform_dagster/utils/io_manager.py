from dagster import IOManager
import awswrangler as wr
import pandas as pd


class AwsWranglerDeltaLakeIOManager(IOManager):
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    def handle_output(self, context, obj):
        if not isinstance(obj, pd.DataFrame):
            raise TypeError("This IO Manager only supports pandas DataFrames")

        # Use the asset name as the table name
        table_name = context.asset_key.path[-1]
        table_path = f"s3://{self.bucket_name}/{table_name}/"

        # Write the DataFrame to Delta Lake format in S3
        wr.s3.to_deltalake(
            df=obj,
            path=table_path,
            mode="overwrite",
            s3_allow_unsafe_rename=True
        )

        context.log.info(f"Data written to Delta Lake table at {table_path}")

    def load_input(self, context):
        table_name = context.asset_key.path[-1]
        table_path = f"s3://{self.bucket_name}/{table_name}/"

        # Read the Delta Lake table from S3
        df = wr.s3.read_deltalake(table_path)
        return df
