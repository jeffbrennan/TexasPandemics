from datetime import datetime as dt

import pandas as pd

import src.utils
from src.wastewater.houston_wastewater_common import (
    get_offsets,
    get_data_manager,
    run_diagnostics,
    get_max_timestamp
)


def handle_output(df: pd.DataFrame) -> None:
    run_diagnostics(df=df, id_col='Plant_Name')
    src.utils.write_file(df, 'tableau/wastewater/houston_plant_wastewater')


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    clean_df = (
        df
        .query('corname.str.upper() != "Z.TOTAL"')
        .rename(
            columns={
                'corname': 'Plant_Name',
                'vl_est': 'viral_load_pct',
                'spline_ww': 'viral_load_log10',
                'firstdate': 'date_first',
                'lastdate': 'date_last',
            }
        )
        .assign(Date=lambda x: pd.to_datetime(x['date'] * 1_000_000))
        .assign(County='Harris')
        .assign(viral_load=lambda x: 10 ** x['viral_load_log10'])
        [['County', 'Plant_Name', 'Date', 'viral_load_log10', 'viral_load', 'viral_load_pct']]
        .sort_values(['Plant_Name', 'Date'])
    )

    return clean_df


def manage_houston_plant_wastewater() -> pd.DataFrame:
    # region  --------------------------------------------------------------------------------
    request_url = 'https://services.arcgis.com/lqRTrQp2HrfnJt8U/ArcGIS/rest/services/WWTP_gdb/FeatureServer/0/query?where=1%3D1&outFields=date%2C+corname%2C+vl_est%2C+spline_ww%2C+firstdate%2C+lastdate&sqlFormat=none&f=pjson&token=&resultOffset='

    current_max_date_as_timestamp = get_max_timestamp(None)
    # endregion

    # region  --------------------------------------------------------------------------------
    offsets = get_offsets(request_url=request_url, step_interval=2000)
    new_dfs_combined = get_data_manager(
        url=request_url,
        offsets=offsets,
        max_date=current_max_date_as_timestamp
    )
    assert new_dfs_combined.empty is False, 'No data found'
    # endregion

    # region  --------------------------------------------------------------------------------
    clean_df = clean_data(new_dfs_combined)
    return clean_df
