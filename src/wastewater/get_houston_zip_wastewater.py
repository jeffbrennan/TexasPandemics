import pandas as pd

import src.utils
from src.wastewater.houston_wastewater_common import (
    get_offsets,
    get_data_manager,
    run_diagnostics,
    get_max_timestamp
)


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
    request_url = 'https://services.arcgis.com/lqRTrQp2HrfnJt8U/ArcGIS/rest/services/Wastewater_Zip_Case_Analysis/FeatureServer/0//query?where=1%3D1&outFields=date%2C+ZIPCODE%2C+pop%2C+Spline_PR%2C+Spline_WW_Weight%2C+Spline_WW_weight_Percent_10&f=pjson&token=&resultOffset='

    current_max_date_timestamp = get_max_timestamp(None)
    # endregion

    # region  --------------------------------------------------------------------------------
    offsets = get_offsets(request_url, step_interval=1000)
    new_dfs_combined = get_data_manager(
        url=request_url,
        offsets=offsets,
        max_date=current_max_date_timestamp
    )
    assert new_dfs_combined.empty is False, 'No data found'
    # endregion

    # region  --------------------------------------------------------------------------------
    clean_df = clean_data(new_dfs_combined)
    run_diagnostics(clean_df, id_col='zipcode')
    src.utils.write_file(clean_df, 'tableau/wastewater/houston_zip_wastewater')


houston_zip_wastewater()
