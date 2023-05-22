import pandas as pd


def write_file(df: pd.DataFrame, table_path: str) -> None:
    print(f'Writing file to {table_path}')
    df.to_csv(f'{table_path}.csv', index=False)


def load_csv(url: str) -> pd.DataFrame:
    df = pd.read_csv(url)
    return df
