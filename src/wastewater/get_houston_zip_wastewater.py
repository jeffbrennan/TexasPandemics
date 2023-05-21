import requests
import pandas as pd
import json

import src.utils
from datetime import date, datetime


def houston_zip_wastewater():
    def get_data(offset):
        url_prefix = 'https://services.arcgis.com/lqRTrQp2HrfnJt8U/ArcGIS/rest/services/Wastewater_Zip_Case_Analysis/FeatureServer/0/query?where=1%3D1&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=date+desc&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset='
        url_suffix = '&resultRecordCount=&sqlFormat=none&f=pjson&token='

        url = f'{url_prefix}{offset}{url_suffix}'

        request = requests.get(url)
        response = json.loads(request.content)['features']
        df = pd.DataFrame.from_records(i['attributes'] for i in response)
        return df

    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        clean_df = (
            df
            .rename(
                columns={
                    'ZIPCODE': 'zipcode',
                    'pop': 'population_served',
                    'Spline_PR': 'positivity_rate',
                    'Spline_WW_weight': 'viral_load_log10',
                    'Spline_WW_weight_Percent_10': 'viral_load_pct_vs_baseline'
                }
            )
            .assign(Date=lambda x: pd.to_datetime(df.date * 1_000_000))
            .assign(zipcode=lambda x: (pd.to_numeric(x['zipcode'], errors='coerce'))
            .astype({'zipcode': 'Int32'}))
            .dropna(subset=['Date', 'zipcode'])
            .sort_values(['zipcode', 'Date'])
            [[
                'Date', 'zipcode', 'population_served',
                'positivity_rate',
                'viral_load_log10', 'viral_load_pct_vs_baseline'
            ]]
        )

        return clean_df

    def get_offsets() -> list:
        def get_num_records() -> int:
            request_url = 'https://services.arcgis.com/lqRTrQp2HrfnJt8U/arcgis/rest/services/Wastewater_Zip_Case_Analysis/FeatureServer/0/query?where=1%3D1&objectIds=&time=&resultType=none&outFields=count%28*%29+as+n&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson&token='
            response = requests.get(request_url)
            output = json.loads(response.content)['features'][0]['attributes']['n']
            return output

        offsets = list(range(0, get_num_records(), 1000))
        return offsets

    def get_raw_data_manager(offsets: list, max_date: str) -> list:
        new_dfs = []
        for offset in offsets:
            print(f'Obtaining data with offset: {offset}')

            df = get_data(offset)

            # filtering by date/timestamp in rest query wasn't working
            df_new = (
                df
                .query('date > @max_date')
            )
            if df_new.empty:
                break

            new_dfs.append(df_new)
        return new_dfs

    def run_diagnostics(df: pd.DataFrame) -> None:
        check_if_zipcode_null = df.zipcode.isnull().any()
        assert not check_if_zipcode_null, 'Null zipcode found'

    # region  --------------------------------------------------------------------------------
    current_max_date = datetime(1999, 12, 31)
    current_max_date_as_timestamp = int(current_max_date.timestamp() * 1000)
    # endregion

    # region  --------------------------------------------------------------------------------
    offsets = get_offsets()
    new_dfs = get_raw_data_manager(offsets, current_max_date_as_timestamp)
    assert len(new_dfs) > 0, 'No new data found'
    # endregion
    
    # region  --------------------------------------------------------------------------------
    new_dfs_combined = pd.concat(new_dfs)
    clean_df = clean_data(new_dfs_combined)

    run_diagnostics(clean_df)
    src.utils.write_file(clean_df, 'tableau/wastewater/houston_zip_wastewater')


houston_zip_wastewater()
