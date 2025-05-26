import pyodbc
import pandas as pd


def load_sql(
    cursor: pyodbc.Cursor, sql: str,
):
    cursor.execute(sql)


def load_df_to_table(
    cursor: pyodbc.Cursor,
    df: pd.DataFrame,
    table_name: str,
    batch_size: int = 5000
):
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['?'] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = df.iloc[start:end].values.tolist()
        cursor.executemany(insert_sql, batch)

    print(f"{table_name} loaded successfully")
