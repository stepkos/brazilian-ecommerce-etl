import pandas as pd
import uuid
from unidecode import unidecode


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

    return transformed
