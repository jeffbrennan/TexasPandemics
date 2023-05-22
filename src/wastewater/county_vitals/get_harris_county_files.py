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

    url_prefix = 'https://services.arcgis.com/su8ic9KbA7PYVxPS/arcgis/rest/services/Download_Reported_COVID_Cases_Timeline/FeatureServer/0/query?where=1%3D1&outFields=Date as date,Total,Source&outSR=4326&f=json&resultOffset='
    num_records_request = 'https://services.arcgis.com/su8ic9KbA7PYVxPS/arcgis/rest/services/Download_Reported_COVID_Cases_Timeline/FeatureServer/0/query?where=1%3D1&outFields=count(*) as n&returnGeometry=false&outSR=4326&f=json'


    current_max_date = pd.read_csv('tableau/vitals/staging/harris_vitals.csv')['Date'].max()
    current_max_date_timestamp = int(dt.strptime(current_max_date, '%Y-%m-%d').timestamp() * 1000)

    offsets = get_offsets(
        request_url=num_records_request,
        step_interval=2000
    )

    raw_data = get_data_manager(
        url_prefix=url_prefix,
        url_suffix='',
        offsets=offsets,
        max_date=current_max_date_timestamp
    )

    if raw_data is None:
        print(f'No new data found using max date: {current_max_date}')
        return

    clean_df = clean_data(raw_data)
    run_diagnostics(clean_df, id_col='County')

    src.utils.write_file(clean_df, 'tableau/vitals/staging/harris_vitals')


get_harris_vitals('Vitals - Harris')
