import pandas as pd


def transform_products(
    products: pd.DataFrame,
    products_category_name_translation: pd.DataFrame
):
    merged = products.merge(
        products_category_name_translation,
        on='product_category_name',
        how='left'
    )
    merged['product_name_length'] = merged['product_name_lenght'].fillna(0).astype(int)
    merged['product_description_length'] = merged['product_description_lenght'].fillna(0).astype(int)
    merged['product_photos_qty'] = merged['product_photos_qty'].fillna(0).astype(int)
    merged['product_weight_g'] = merged['product_weight_g'].fillna(0).astype(int)
    merged['product_length_cm'] = merged['product_length_cm'].fillna(0).astype(int)
    merged['product_height_cm'] = merged['product_height_cm'].fillna(0).astype(int)
    merged['product_width_cm'] = merged['product_width_cm'].fillna(0).astype(int)

    merged = merged.drop(['product_name_lenght', 'product_description_lenght'], axis=1)
    merged = merged.where(pd.notnull(merged), None)
    return merged
