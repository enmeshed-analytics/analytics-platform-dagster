
def process_chunk(df_chunk, conn):
    """
    Takes dataframe and connection
    Inserts df into table
    """
    conn.execute("""INSERT INTO open_usrns_table SELECT * usrn FROM df_chunk""")