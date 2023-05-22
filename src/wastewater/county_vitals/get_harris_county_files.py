import pandas as pd
import src.utils
from src.wastewater.houston_wastewater_common import (
    get_offsets,
    get_data_manager,
    run_diagnostics,
    get_max_timestamp
)

from datetime import datetime as dt


def get_harris_vitals(data_type: str) -> None:
    print(f'================Obtaining {data_type}==============================')

    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        clean_df = (
            df
            .query('Source == "HCTX"')
            .rename(
                columns={
                    'Total': 'cases_daily'
                }
            )
            .assign(Date=lambda x: pd.to_datetime(x['date'] * 1_000_000).dt.date)
            .drop('date', axis=1)
            .dropna(subset=['Date', 'cases_daily'])
            .assign(County='Harris')
            [['County', 'Date', 'cases_daily']]
            .sort_values(['Date'])

        )

        return clean_df

    request_url = 'https://services.arcgis.com/su8ic9KbA7PYVxPS/arcgis/rest/services/Download_Reported_COVID_Cases_Timeline/FeatureServer/0/query?where=1%3D1&outFields=Date as date,Total,Source&outSR=4326&f=json&resultOffset='

    current_max_date_timestamp = get_max_timestamp('tableau/vitals/staging/harris_vitals.csv')
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
        return

    clean_df = clean_data(raw_data)
    run_diagnostics(clean_df, id_col='County')

    src.utils.write_file(clean_df, 'tableau/vitals/staging/harris_vitals')


get_harris_vitals('Vitals - Harris')
