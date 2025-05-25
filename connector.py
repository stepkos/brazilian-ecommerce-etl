import pyodbc

server = 'localhost'
database = 'AdventureWorks2019'

conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'Trusted_Connection=yes;'
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("SELECT 1")
rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()
