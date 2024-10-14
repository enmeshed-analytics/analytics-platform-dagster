import boto3
import streamlit as st
import pandas as pd
from typing import Tuple, Optional

from deltalake import DeltaTable

def list_dirs_in_s3() -> list:
    """
    List all the DeltaLake dirs in S3

    Returns:
        List of available directories - basically the root to the tables
    """
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
            directory_names.append(prefix["Prefix"])
    else:
        print("No directories found in the bucket.")

    return directory_names

def read_delta_table_from_s3(delta_table_path: str, version: Optional[int] = None) -> Tuple[pd.DataFrame, dict]:
    """
    Read a Delta table from S3 and return the data as a DataFrame along with metadata.

    Args:
        delta_table_path (str): The path to the Delta table in S3.
        version (int, optional): The version of the Delta table to read. Defaults to None (latest version).

    Returns:
        tuple: A tuple containing:
            - pd.DataFrame: The Delta table data as a pandas DataFrame.
            - dict: Metadata about the Delta table.
    """
    # Fetch the bucket name from Streamlit secrets
    bucket_name = st.secrets["bucket"]
    # Construct the full S3 path
    s3_uri = f"s3://{bucket_name}/{delta_table_path}"
    # Load the Delta table from S3
    delta_table = DeltaTable(s3_uri, version=version)
    df = delta_table.to_pandas()
    # Retrieve metadata
    metadata = {
        "version": delta_table.version(),
        "metadata": delta_table.metadata(),
        "files": delta_table.files(),
        "history": delta_table.history(),
    }
    return df, metadata

def show_available_dirs() -> str | None:
    """
    Processes directories and displays them in Streamlit for selection.
    """
    # Fetch available directories from S3
    dirs = list_dirs_in_s3()

    # Add a placeholder for no preselection
    options = ["Select a Delta Lake"] + dirs

    # Use columns to make the select box more prominent
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        selected_dir = st.selectbox(
            "",  # Empty label to avoid duplication
            options=options,
            index=0,
            key="prominent_select"
        )

        # Display the selected directory prominently
        if selected_dir != "Select a Delta Lake":
            st.markdown(f"**Selected:** `{selected_dir}`")

    # Return None if the user hasn't selected a valid directory
    if selected_dir == "Select a Delta Lake":
        return None
    else:
        return selected_dir
