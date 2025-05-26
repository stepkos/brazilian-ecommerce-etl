from decimal import Decimal
from pathlib import Path

import pandas as pd
import uuid
from unidecode import unidecode

cache_path = Path(__file__).parent.parent / "cache"


def sanitize_string(s):
    if pd.isna(s):
        return None
    return unidecode(s).lower()


def to_datetime_str(dt):
    if pd.isna(dt):
        return None
    return pd.to_datetime(dt).strftime('%Y%m%d%H')


def transform_products(
    products: pd.DataFrame,
    products_category_name_translation: pd.DataFrame
):
    merged = products.merge(
        products_category_name_translation,
        on='product_category_name',
        how='left'
    )
    merged['product_name_length'] = merged['product_name_lenght'].fillna(0).astype('Int64')
    merged['product_description_length'] = merged['product_description_lenght'].fillna(0).astype('Int64')
    merged['product_photos_qty'] = merged['product_photos_qty'].fillna(0).astype('Int64')
    merged['product_weight_g'] = merged['product_weight_g'].fillna(0).astype('Int64')
    merged['product_length_cm'] = merged['product_length_cm'].fillna(0).astype('Int64')
    merged['product_height_cm'] = merged['product_height_cm'].fillna(0).astype('Int64')
    merged['product_width_cm'] = merged['product_width_cm'].fillna(0).astype('Int64')

    merged = merged.drop(['product_name_lenght', 'product_description_lenght'], axis=1)
    merged = merged.where(pd.notnull(merged), None)
    merged = merged.astype(object).where(pd.notnull(merged), None)
    return merged


def generate_city_id(state: str, city: str) -> str:
    namespace = uuid.NAMESPACE_DNS
    name = f"{state}-{city}"
    return uuid.uuid5(namespace, name).hex


def generate_order_item_id(order_id: str, position: int) -> str:
    namespace = uuid.NAMESPACE_DNS
    name = f"{order_id}-{position}"
    return uuid.uuid5(namespace, name).hex


def transform_cities(cities: pd.DataFrame) -> pd.DataFrame:
    cols_map = {
        'CITY': 'city_name',
        'STATE': 'state_code',
        'CAPITAL': 'is_capital',
        'IBGE_RES_POP': 'ibge_res_pop',
        'IBGE_RES_POP_BRAS': 'ibge_res_pop_bras',
        'IBGE_RES_POP_ESTR': 'ibge_res_pop_estr',
        'IBGE_DU': 'ibge_du',
        'IBGE_DU_URBAN': 'ibge_du_urban',
        'IBGE_DU_RURAL': 'ibge_du_rural',
        'IBGE_POP': 'ibge_pop'
    }

    df = cities[list(cols_map.keys())].rename(columns=cols_map)

    df['is_capital'] = df['is_capital'].astype(bool)
    int_cols = [
        'ibge_res_pop', 'ibge_res_pop_bras', 'ibge_res_pop_estr',
        'ibge_du', 'ibge_du_urban', 'ibge_du_rural', 'ibge_pop'
    ]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    df['city_id'] = df.apply(
        lambda row: generate_city_id(row['state_code'], row['city_name']),
        axis=1
    )
    df = df[[
        'city_id',
        'city_name',
        'state_code',
        'is_capital',
        'ibge_res_pop',
        'ibge_res_pop_bras',
        'ibge_res_pop_estr',
        'ibge_du',
        'ibge_du_urban',
        'ibge_du_rural',
        'ibge_pop',
    ]]
    df = df.where(pd.notnull(df), None)
    df = df.astype(object).where(pd.notnull(df), None)
    return df


def generate_timestamps(start_year=2016, end_year=2018) -> pd.DataFrame:
    dt_range = pd.date_range(
        start=f'{start_year}-01-01 00:00',
        end=f'{end_year}-12-31 23:00',
        freq='h'
    )

    df = pd.DataFrame({'datetime': dt_range})

    df['timestamp'] = df['datetime'].dt.strftime('%Y%m%d%H')
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    df['day'] = df['datetime'].dt.day
    df['hour'] = df['datetime'].dt.hour

    df = df[['timestamp', 'year', 'month', 'day', 'hour']]

    return df


def transform_orders(
    raw_orders: pd.DataFrame,
    raw_customers: pd.DataFrame,
    transformed_cities: pd.DataFrame
) -> pd.DataFrame:
    cache_file = cache_path / f"transformed_orders.pkl"
    if cache_file.exists():
        return pd.read_pickle(cache_file)

    transformed_cities = transformed_cities.copy()

    orders_customers = raw_orders.merge(
        raw_customers[['customer_id', 'customer_unique_id', 'customer_city']],
        on='customer_id',
        how='left'
    )

    orders_customers['customer_city'] = orders_customers['customer_city'].apply(sanitize_string)
    transformed_cities['city_name'] = transformed_cities['city_name'].apply(sanitize_string)

    orders_customers_cities = orders_customers.merge(
        transformed_cities[['city_id', 'city_name']],
        left_on='customer_city',
        right_on='city_name',
        how='left'
    )

    df = orders_customers_cities

    transformed = pd.DataFrame({
        'order_id': df['order_id'],
        'customer_unique_id': df['customer_unique_id'],
        'customer_city_id': df['city_id'],
        'order_status': df['order_status'],
        'order_purchase_timestamp': df['order_purchase_timestamp'].apply(to_datetime_str),
        'order_approved_timestamp': df['order_approved_at'].apply(to_datetime_str),
        'order_delivered_carrier_timestamp': df['order_delivered_carrier_date'].apply(to_datetime_str),
        'order_delivered_customer_timestamp': df['order_delivered_customer_date'].apply(to_datetime_str),
        'order_estimated_delivery_timestamp': df['order_estimated_delivery_date'].apply(to_datetime_str),
    })
    transformed = transformed.drop_duplicates(subset=['order_id'])
    transformed = transformed.where(pd.notnull(transformed), None)
    transformed = transformed.astype(object).where(pd.notnull(transformed), None)
    transformed.to_pickle(cache_file)
    return transformed


def transform_reviews(raw_reviews: pd.DataFrame) -> pd.DataFrame:
    cache_file = cache_path / f"transformed_reviews.pkl"
    if cache_file.exists():
        return pd.read_pickle(cache_file)

    transformed = pd.DataFrame({
        'review_id': raw_reviews['review_id'],
        'order_id': raw_reviews['order_id'],
        'review_score': pd.to_numeric(raw_reviews['review_score'], errors='coerce'),
        # 'review_comment_title': raw_reviews['review_comment_title'].astype(str).where(
        #     raw_reviews['review_comment_title'].notna(), None
        # ),
        # 'review_comment_message': raw_reviews['review_comment_message'].astype(str).where(
        #     raw_reviews['review_comment_message'].notna(), None
        # ),
        'review_creation_timestamp': raw_reviews['review_creation_date'].apply(to_datetime_str),
        'review_answer_timestamp': raw_reviews['review_answer_timestamp'].apply(to_datetime_str),
    })
    transformed = transformed.drop_duplicates(subset=['review_id'])
    transformed = transformed.where(pd.notnull(transformed), None)
    transformed = transformed.astype(object).where(pd.notnull(transformed), None)
    transformed.to_pickle(cache_file)
    return transformed


def transform_order_items(
    raw_order_items: pd.DataFrame,
    extracted_sellers: pd.DataFrame,
    transformed_cities: pd.DataFrame
) -> pd.DataFrame:
    cache_file = cache_path / f"transformed_order_items.pkl"
    if cache_file.exists():
        return pd.read_pickle(cache_file)

    cities = transformed_cities[['city_id', 'city_name']].copy()
    cities['city_name'] = cities['city_name'].apply(sanitize_string)

    sellers_cities = extracted_sellers.merge(
        cities,
        left_on='seller_city',
        right_on='city_name',
        how='left'
    )
    order_items_full = raw_order_items.merge(
        sellers_cities[['seller_id', 'city_id']],
        on='seller_id',
        how='left'
    )
    transformed = pd.DataFrame({
        'order_item_position': order_items_full['order_item_id'].astype('Int64'),
        'order_id': order_items_full['order_id'],
        'product_id': order_items_full['product_id'],
        'seller_id': order_items_full['seller_id'],
        'seller_city_id': order_items_full['city_id'],
        'shipping_limit_timestamp': order_items_full['shipping_limit_date'].apply(to_datetime_str),
        'price': order_items_full['price'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None),
        'freight_value': order_items_full['freight_value'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)
    })
    transformed['order_item_id'] = transformed.apply(
        lambda row: generate_order_item_id(row['order_id'], row['order_item_position']),
        axis=1
    )
    transformed = transformed.drop_duplicates(subset=['order_item_id'])
    transformed = transformed.where(pd.notnull(transformed), None)
    transformed = transformed.astype(object).where(pd.notnull(transformed), None)
    transformed.to_pickle(cache_file)
    return transformed

