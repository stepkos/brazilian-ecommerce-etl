import pandas as pd
from pathlib import Path

cache_path = Path(__file__).parent.parent / "cache"
data_path = Path(__file__).parent.parent / "data"


def extract_csv(file_path: str, cache: bool = True) -> pd.DataFrame:
    cache_file = cache_path / f"{Path(file_path).stem}.pkl"
    if cache and cache_file.exists():
        print(f"Loading cached data from {cache_file}")
        return pd.read_pickle(cache_file)

    file_path = data_path / file_path
    print(f"Extracting data from {file_path}")
    df = pd.read_csv(file_path, na_values=[''])
    if cache:
        df.to_pickle(cache_file)
    return df


if __name__ == "__main__":
    df = extract_csv("products.csv", cache=True)
    print(df.head())
