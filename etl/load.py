import pyodbc
import pandas as pd


def load_sql(
    cursor: pyodbc.Cursor, sql: str,
):
    cursor.execute(sql)
    return cursor.fetchall()


def load_df_to_table(
    cursor: pyodbc.Cursor,
    df: pd.DataFrame,
    table_name: str,
):
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['?'] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.executemany(insert_sql, df.values.tolist())
