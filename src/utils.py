import pandas as pd
from datetime import datetime as dt


def write_file(df: pd.DataFrame, table_path: str, add_date: bool = True) -> None:
    print(f'Writing file to {table_path}')
    if add_date:
        current_date = dt.now().strftime('%Y_%m_%d')
        df.to_csv(f'{table_path}_{current_date}.csv', index=False)
    else:
        df.to_csv(f'{table_path}.csv', index=False)


def load_csv(url: str) -> pd.DataFrame:
    df = pd.read_csv(url)
    return df


def load_parquet(url: str) -> pd.DataFrame:
    return pd.read_parquet(url)


def convert_date(date_series: pd.Series, date_format: str) -> pd.Series:
    if date_format == 'timestamp_int':
        return pd.to_datetime(date_series * 1_000_000).dt.date

    return pd.to_datetime(date_series, format=date_format).dt.date
