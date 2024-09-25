import streamlit as st
from functions.delta_lake import show_available_dirs, read_delta_table_from_s3
import io


def show_df_logic():
    selected_dir = show_available_dirs()
    if selected_dir:
        try:
            df = read_delta_table_from_s3(selected_dir)

            st.subheader(f"Data from {selected_dir}:")

            # Display first few rows
            st.write("First Few Rows:")
            st.write(df.head())

            # Display basic DataFrame info
            st.write("Basic Info:")
            buffer = io.StringIO()
            df.info(buf=buffer)
            s = buffer.getvalue()
            st.text(s)

            # Display column types
            st.write("Column Types:")
            st.write(df.dtypes)

            # Display missing values
            st.write("Columns with Missing Values:")
            missing_data = df.isnull().sum()
            st.write(missing_data[missing_data > 0])

        except Exception as e:
            st.error(f"""
                Error reading Delta table: {str(e)}
                \n Make sure you select a table from the list!
                """)
    else:
        st.info("Please select a directory to view data.")


def main():
    st.title("Enmeshed Data Lake Checker 🔍")
    st.write("This checks the Silver Bucket only.")

    show_df_logic()


if __name__ == "__main__":
    main()
