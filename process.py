from connector import connector
from sql.create_tables import CREATE_TABLES_SQL
from sql.drop_tables import DROP_TABLES_SQL

cursor = connector.get_cursor()

# Drop tables
for drop_sql in DROP_TABLES_SQL:
    cursor.execute(drop_sql)

# Create tables
for create_sql in CREATE_TABLES_SQL:
    cursor.execute(create_sql)

# 1.
# PRODUCTS
# CITIES
# TIMESTAMPS (based on DIM_ORDERS, FACT_ORDER_ITEMS, DIM_REVIEWS)

# 2.
# ORDERS
# REVIEWS
# ORDER_ITEMS

connector.close()


