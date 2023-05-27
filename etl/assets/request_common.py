from src.utils import convert_date
import pandas as pd


def clean_request_data(df: pd.DataFrame, config) -> pd.DataFrame:
    agg_dict = {col: 'sum' for col in config['col']['metric_out']}

    clean_df = (
        df
        .astype(config['col']['dtypes'])
        .rename(columns=config['col']['rename'])
        .assign(
            Date=lambda x: convert_date(
                date_series=x['Date'],
                date_format=config['col']['date_format'])
        )
        .dropna(subset=config['col']['uid'])
        .assign(County=config['county'])
        .groupby(['County', 'Date'], as_index=False)
        .agg(agg_dict)
    )

    clean_df = clean_df[config['col']['output']]

    return clean_df
