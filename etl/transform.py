import pandas as pd
import uuid


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

    return df
