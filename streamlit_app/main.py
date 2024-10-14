import io
import streamlit as st
import pandas as pd
from datetime import datetime
from functions.delta_lake import show_available_dirs, read_delta_table_from_s3

def convert_timestamp(timestamp):
    """
    DeltaLake metadata provides a unix timestamp that needs to be converted to a readable format.
    """
    try:
        return datetime.fromtimestamp(timestamp / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return f"Invalid: {timestamp}"

def show_df_logic():
    st.markdown("""
    ### Navigate:
    - [Available Directories](#available-directories)
    - [Version Selection](#version-selection)
    - [DataFrame Head](#dataframe-head)
    - [Basic Info](#basic-info)
    - [Column Types](#column-types)
    - [Missing Values](#missing-values)
    - [Delta Lake Metadata](#delta-lake-metadata)
        - [Latest Table Version](#table-version)
        - [Delta Lake Table History](#table-history)
    """)
    st.markdown("<a name='available-directories'></a>", unsafe_allow_html=True)
    selected_dir = show_available_dirs()
    if selected_dir:
        try:
            # First, read the latest version to get metadata
            df, metadata = read_delta_table_from_s3(selected_dir)

            st.markdown("<a name='version-selection'></a>", unsafe_allow_html=True)
            current_version = metadata['version']

            # Create a list of available versions
            available_versions = list(range(current_version, -1, -1))
            selected_version = st.selectbox("Select version", available_versions, index=0)

            # Always read the selected version
            df, metadata = read_delta_table_from_s3(selected_dir, version=selected_version)

            st.markdown(f"### Showing Data from {selected_dir} (Version {selected_version}):")

            st.markdown("<a name='dataframe-head'></a>", unsafe_allow_html=True)
            st.markdown("### DataFrame Head:")
            st.write(df.head(100))

            st.markdown("<a name='basic-info'></a>", unsafe_allow_html=True)
            st.markdown("### Basic Info:")
            buffer = io.StringIO()
            df.info(buf=buffer)
            s = buffer.getvalue()
            st.text(s)

            st.markdown("<a name='column-types'></a>", unsafe_allow_html=True)
            st.markdown("### Column Types:")
            st.write(df.dtypes)

            st.markdown("<a name='missing-values'></a>", unsafe_allow_html=True)
            st.markdown("### Columns with Missing Values:")
            missing_data = df.isnull().sum()
            st.write(missing_data[missing_data > 0])

            st.markdown("<a name='delta-lake-metadata'></a>", unsafe_allow_html=True)
            st.markdown("### Delta Lake Metadata:")

            st.markdown("<a name='table-version'></a>", unsafe_allow_html=True)
            st.markdown("### The Current Table Version:")
            st.write(metadata['version'])

            st.markdown("<a name='table-history'></a>", unsafe_allow_html=True)
            st.markdown("### Table History:")
            history = metadata['history']
            # Convert the history to a DataFrame if it's not already
            if not isinstance(history, pd.DataFrame):
                history_df = pd.DataFrame(history)
            else:
                history_df = history.copy()
            # Convert timestamps in the history DataFrame
            if 'timestamp' in history_df.columns:
                history_df['timestamp'] = history_df['timestamp'].apply(convert_timestamp)
            # Reorder columns for better readability
            column_order = ['timestamp', 'version', 'operation', 'userMetadata']
            history_df = history_df.reindex(columns=[col for col in column_order if col in history_df.columns] +
                                            [col for col in history_df.columns if col not in column_order])
            # Display the DataFrame
            st.dataframe(history_df)
        except Exception as e:
            st.error(f"""
                Error reading Delta table: {str(e)}
                \n Make sure you select a table from the list!
                """)
    else:
        st.warning("Please select a directory to view data.")

def main():
    """
    Launch DeltaLake Browser
    """
    st.set_page_config(layout="wide")
    st.title("Enmeshed Data Lake Browser 🧐")
    st.subheader("This checks the Silver Bucket only.")
    st.subheader("To view the Dagster logs go here: https://dagster-admin.analyticsplatform.co/locations/analytics_platform_dagster/assets")
    show_df_logic()

if __name__ == "__main__":
    main()
