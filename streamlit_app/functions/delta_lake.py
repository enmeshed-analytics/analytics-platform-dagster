import boto3
import streamlit as st

# from pprint import pprint
from deltalake import DeltaTable


def list_dirs_in_s3() -> list:
    # Fetch the bucket name from Streamlit secrets
    bucket_name = st.secrets["bucket"]

    # Create an S3 client
    s3_client = boto3.client("s3")

    # Use list_objects_v2 to list directories in the bucket with the delimiter
    response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter="/")

    # Create an empty list to hold the directory names
    directory_names = []

    # If the response contains 'CommonPrefixes', it has found directories
    if "CommonPrefixes" in response:
        for prefix in response["CommonPrefixes"]:
            directory_names.append(prefix["Prefix"])  # Only append directory names
    else:
        print("No directories found in the bucket.")

    return directory_names


def list_dir_contents(directory_name: str):
    # Fetch the bucket name from Streamlit secrets
    bucket_name = st.secrets["bucket"]

    # Create an S3 client
    s3_client = boto3.client("s3")

    # Use list_objects_v2 to list contents in the specified directory (prefix)
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_name)

    # Create an empty list to hold the object names within the directory
    object_names = []

    # Check if the response contains any objects in the directory
    if "Contents" in response:
        for obj in response["Contents"]:
            object_names.append(obj["Key"])  # Append the full path (key) of each object
    else:
        print(f"No objects found in the directory: {directory_name}")

    return object_names


def read_delta_table_from_s3(delta_table_path):
    # Fetch the bucket name from Streamlit secrets
    bucket_name = st.secrets["bucket"]

    # Construct the full S3 path
    s3_uri = f"s3://{bucket_name}/{delta_table_path}"

    # Load the Delta table from S3
    delta_table = DeltaTable(s3_uri)

    # Read the Delta table into a pandas DataFrame
    df = delta_table.to_pandas()

    return df


def show_available_dirs():
    # Fetch available directories from S3
    dirs = list_dirs_in_s3()

    # Add a placeholder for no preselection (e.g., "Select a directory")
    selected_dir = st.selectbox(
        "Select a directory", options=["Select a Delta Lake"] + dirs, index=0
    )

    # Return None if the user hasn't selected a valid directory
    if selected_dir == "Select a directory":
        return None
    else:
        return selected_dir


# if __name__ == "__main__":
#     var = list_dirs_in_s3()
#     dir = var[0]
#     var2 = read_delta_table_from_s3(dir)
#     pprint(var2)
