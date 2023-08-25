import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
import subprocess


def write_file(df: pd.DataFrame, table_path: str, add_date: bool = True) -> None:
    print(f'Writing file to {table_path}')
    if add_date:
        current_date = dt.now().strftime('%Y_%m_%d')
        df.to_csv(f'{table_path}_{current_date}.csv', index=False)
    else:
        df.to_csv(f'{table_path}.csv', index=False)


def load_csv(url: str, skip_rows: int = 0) -> pd.DataFrame:
    df = pd.read_csv(url, skiprows=skip_rows)
    return df


def load_parquet(url: str) -> pd.DataFrame:
    return pd.read_parquet(url)


def convert_date(date_series: pd.Series, date_format: str) -> pd.Series:
    if date_format == 'timestamp_int':
        return pd.to_datetime(date_series * 1_000_000).dt.date

    return pd.to_datetime(date_series, format=date_format).dt.date


# TODO: generalize config args
def run_r_script(config: dict) -> bool:
    # https://stackoverflow.com/questions/62716500/call-and-execute-an-r-script-from-python
    print(f'Running R script: {config["script_path"]}')
    try:
        p = subprocess.Popen(
            ["Rscript",
             config['arg'],
             config['script_path'],
             config['input_file'],
             config['output_file']
             ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        output, error = p.communicate()

        if p.returncode == 0:
            print('R OUTPUT:\n {0}'.format(output.decode("utf-8")))
        else:
            print('R ERROR:\n {0}'.format(error.decode("utf-8")))

        return True

    except Exception as e:
        print(e)

        return False


def union_df_list(df_list: list) -> pd.DataFrame:
    combined_df = (
        pd.concat(df_list)
        .reset_index(drop=True)
        .assign(Date=lambda df: pd.to_datetime(df.Date))
    )
    return combined_df


def filter_df_date(df: pd.DataFrame, days: int = 14) -> pd.DataFrame:
    min_date = max(df['Date']) - timedelta(days=days)
    return df.query("Date >= @min_date")
