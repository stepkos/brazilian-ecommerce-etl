from connector import connector
from sql.create_tables import CREATE_TABLES_SQL
from sql.drop_tables import DROP_TABLES_SQL

cursor = connector.get_cursor()

for drop_sql in DROP_TABLES_SQL:
    cursor.execute(drop_sql)

for create_sql in CREATE_TABLES_SQL:
    cursor.execute(create_sql)

connector.close()
