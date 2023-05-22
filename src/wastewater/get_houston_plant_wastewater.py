from datetime import datetime as dt

import pandas as pd

import src.utils
from src.wastewater.houston_wastewater_common import (
    get_offsets,
    get_data_manager,
    run_diagnostics,
    get_max_timestamp
)


def houston_plant_wastewater() -> None:
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

    # region  --------------------------------------------------------------------------------
    request_url_prefix = 'https://services.arcgis.com/lqRTrQp2HrfnJt8U/ArcGIS/rest/services/WWTP_gdb/FeatureServer/0//query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=none&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=date&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset='
    request_url_suffix = '&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token='
    num_records_request_url = 'https://services.arcgis.com/lqRTrQp2HrfnJt8U/ArcGIS/rest/services/WWTP_gdb/FeatureServer/0//query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=count%28*%29+as+n&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=none&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=standard&f=pjson&token='

    current_max_date = dt(1999, 12, 31)
    current_max_date_as_timestamp = int(current_max_date.timestamp() * 1000)
    # endregion

    # region  --------------------------------------------------------------------------------
    offsets = get_offsets(request_url=num_records_request_url, step_interval=2000)
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
    run_diagnostics(df=clean_df, id_col='Plant_Name')
    src.utils.write_file(clean_df, 'tableau/wastewater/houston_plant_wastewater')
    # endregion


houston_plant_wastewater()
