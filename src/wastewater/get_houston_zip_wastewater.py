import requests
import pandas as pd
import json

import src.utils
from src.wastewater.houston_wastewater_common import get_offsets, get_data_manager, run_diagnostics
from datetime import date, datetime


def houston_zip_wastewater():
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

    # region  --------------------------------------------------------------------------------
    num_records_request_url = 'https://services.arcgis.com/lqRTrQp2HrfnJt8U/arcgis/rest/services/Wastewater_Zip_Case_Analysis/FeatureServer/0/query?where=1%3D1&objectIds=&time=&resultType=none&outFields=count%28*%29+as+n&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson&token='
    request_url_prefix = 'https://services.arcgis.com/lqRTrQp2HrfnJt8U/ArcGIS/rest/services/Wastewater_Zip_Case_Analysis/FeatureServer/0/query?where=1%3D1&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=date+desc&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset='
    request_url_suffix = '&resultRecordCount=&sqlFormat=none&f=pjson&token='

    current_max_date = datetime(1999, 12, 31)
    current_max_date_as_timestamp = int(current_max_date.timestamp() * 1000)
    # endregion

    # region  --------------------------------------------------------------------------------
    offsets = get_offsets(request_url=num_records_request_url, step_interval=1000)
    new_dfs_combined = get_data_manager(
        request_url_prefix,
        request_url_suffix,
        offsets,
        current_max_date_as_timestamp
    )
    assert new_dfs_combined.empty is False, 'No data found'
    # endregion

    # region  --------------------------------------------------------------------------------
    clean_df = clean_data(new_dfs_combined)
    run_diagnostics(clean_df, id_col='zipcode')
    src.utils.write_file(clean_df, 'tableau/wastewater/houston_zip_wastewater')


houston_zip_wastewater()
