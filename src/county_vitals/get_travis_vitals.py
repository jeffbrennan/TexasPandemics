from datetime import datetime as dt

import pandas as pd

import src.utils
from src.wastewater.houston_wastewater_common import (
    get_offsets,
    get_data_manager,
    run_diagnostics,
    get_max_timestamp
)


def get_travis_vitals(data_type: str) -> None:
    print(f'================Obtaining {data_type}==============================')

    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        clean_df = (
            df
            .rename(
                columns={
                    'TotalCases': 'cases_daily',
                    'TotalDeaths': 'deaths_daily'
                }
            )
            .assign(Date=lambda x: pd.to_datetime(x['date'] * 1_000_000).dt.date)
            .drop('date', axis=1)
            .dropna(subset=['Date'])
            .assign(County='Travis')
            [['County', 'Date', 'cases_daily', 'deaths_daily']]
            .sort_values(['Date'])
        )

        return clean_df

    request_url = 'https://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/Daily_Count_COVID_view/FeatureServer/0/query?where=1%3D1&outFields=Updated as date,TotalCases,TotalDeaths&outSR=4326&f=json&resultOffset='

    current_max_date_timestamp = get_max_timestamp(None)
    offsets = get_offsets(
        request_url=request_url,
        step_interval=2000
    )

    raw_data = get_data_manager(
        url=request_url,
        offsets=offsets,
        max_date=current_max_date_timestamp
    )

    if raw_data is None:
        print(f'No new data found')
        return None

    clean_df = clean_data(raw_data)
    run_diagnostics(clean_df, id_col='County')

    src.utils.write_file(clean_df, 'tableau/vitals/staging/travis_vitals')


get_travis_vitals('Vitals - Travis')
