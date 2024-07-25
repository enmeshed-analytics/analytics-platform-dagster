
def process_chunk_os_usrn(df_chunk, conn):
    """
    Function to insert OS open usrn data into duckdb

    Args:
        DataFrame
        connection object

    Returns:
        None
    """
    conn.execute("""INSERT INTO open_usrns_table SELECT * usrn FROM df_chunk""")
