# from connector import connector
from etl.extract import extract_csv
# from etl.load import load_sql, load_df_to_table
from etl.transform import transform_products, transform_cities, generate_timestamps, transform_orders, \
    transform_reviews, transform_order_items
from sql.create_tables import CREATE_TABLES_SQL
from sql.drop_tables import DROP_TABLES_SQL

# Extract
extracted_products = extract_csv("products.csv", cache=True)
extracted_product_category_name_translation = extract_csv(
    "product_category_name_translation.csv", cache=True
)
extracted_cities = extract_csv(
    "brazil_cities.csv", cache=True, delimiter=";"
)
extracted_orders = extract_csv("orders.csv", cache=True)
extracted_customers = extract_csv("customers.csv", cache=True)
extracted_reviews = extract_csv("order_reviews.csv", cache=True)
extracted_sellers = extract_csv("sellers.csv", cache=True)
extracted_order_items = extract_csv("order_items.csv", cache=True)

print("All data extracted successfully")

# Transform
transformed_products = transform_products(
    extracted_products,
    extracted_product_category_name_translation
)
print("Products transformed successfully")

transformed_cities = transform_cities(extracted_cities)
print("Cities transformed successfully")

transformed_timestamps = generate_timestamps(2016, 2018)
print("Timestamps generated successfully")

transformed_orders = transform_orders(
    extracted_orders,
    extracted_customers,
    transformed_cities
)
print("Orders transformed successfully")

transformed_reviews = transform_reviews(extracted_reviews)
print('Reviews transformed  successfully')

transformed_order_items = transform_order_items(
    extracted_order_items, extracted_sellers, transformed_cities, transformed_orders, transformed_reviews
)
print("Order items transformed successfully")
print("All data transformed successfully")

# Load
# try:
#     cursor = connector.get_cursor()
#     # Drop tables
#     for drop_sql in DROP_TABLES_SQL:
#         load_sql(cursor, drop_sql)
#
#     # Create tables
#     for create_sql in CREATE_TABLES_SQL:
#         load_sql(cursor, create_sql)
#
#     cursor.fast_executemany = True
#
#     # Load products
#     load_df_to_table(cursor, transformed_products, 'DIM_PRODUCTS')
#
#     # Load cities
#     load_df_to_table(cursor, transformed_cities, 'DIM_CITIES')
#
#     # Load timestamps
#     load_df_to_table(cursor, transformed_timestamps, 'DIM_TIMESTAMP')

#     # Load reviews
#     load_df_to_table(cursor, transformed_reviews, 'DIM_REVIEWS')

#     # Load order items
#     load_df_to_table(cursor, transformed_order_items, 'FACT_ORDER_ITEMS')
#
#     connector.conn.commit()
# except Exception as e:
#     connector.conn.rollback()
#     print("Failed to load data")
#     raise e
# finally:
#     connector.close()
