from sqlalchemy import create_engine
import pandas as pd
from tqdm import tqdm

def get_sqlalchemy_engine(server, database):
    connection_str = (
        f"mssql+pyodbc://{server}/{database}?"
        "driver=ODBC+Driver+17+for+SQL+Server&"
        "trusted_connection=yes"
    )
    engine = create_engine(connection_str)
    return engine

def get_order_by_column(engine, schema, table):
    query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
        AND TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    """

    with engine.connect() as connection:
        result = connection.execute(query).fetchone()

        if result:
            return result[0]
        else:
            query = f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
                ORDER BY ORDINAL_POSITION
            """
            result = connection.execute(query).fetchone()
            return result[0] if result else None

def getData(database, schema, table, page_size=10000, sample=False):
    """
    Fetches all data or a sample from a SQL Server table using pagination and returns a pandas DataFrame.

    Args:
        database (str): The name of the database.
        schema (str): The schema name of the table.
        table (str): The table name to query.
        page_size (int): The number of rows to fetch in one batch (default is 10000).
        sample (bool): If True, only fetch the first 100 rows (default is False).

    Returns:
        pd.DataFrame: A pandas DataFrame containing the requested rows of the table.
    """
    server = "udpdev.ad.rice.edu\\udp"
    engine = get_sqlalchemy_engine(server, database)
    print("[UDP] Connection Established")

    # Find the primary key or other column to order by
    order_by_column = get_order_by_column(engine, schema, table)

    if not order_by_column:
        raise ValueError("Could not determine a suitable column to order by.")

    all_data = []
    offset = 0

    # Initialize a progress bar with `tqdm`
    with engine.connect() as connection:
        # Estimate the total number of rows for progress tracking
        query_count = f"SELECT COUNT(*) FROM {schema}.{table}"
        total_rows = connection.execute(query_count).scalar()
    
    if sample:
        # Adjust page size if only a sample is needed
        total_pages = 1
        page_size = min(100, total_rows)
    else:
        total_pages = (total_rows + page_size - 1) // page_size

    for _ in tqdm(range(total_pages), desc="[UDP] Fetching data", unit="page"):
        query = f"""
            SELECT * FROM {schema}.{table}
            ORDER BY {order_by_column}
            OFFSET {offset} ROWS
            FETCH NEXT {page_size} ROWS ONLY
        """
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)

        if df.empty:
            break

        all_data.append(df)
        offset += page_size

    # Concatenate all pages into a single DataFrame
    final_df = pd.concat(all_data, ignore_index=True)
    print('[UDP] Data Fetched')
    return final_df

# Example usage
# database = "udpdb"
# schema = "bea"
# table = "rea_012021_tables"
# df = getData(database = database, schema = schema, table = table, sample=False)
