import pandas as pd
from dagster import asset

from src.get_covid_dshs import (
    get_result_table,
    fix_table_names,
    get_report_date,
    apply_metric_fixes,
    reshape_df,
    clean_pivoted_df
)


@asset(
    name="new_texas_vitals",
    group_name="vitals_state",
    key_prefix=['vitals', 'state'],
    metadata={
        "schema": "origin/vitals_state",
        "table_name": "new_texas_vitals",
        "add_archive": True
    },
    io_manager_key='pandas_io_manager'
)
def new_texas_vitals() -> pd.DataFrame:
    dshs_base_url = 'https://www.dshs.texas.gov/covid-19-coronavirus-disease/texas-covid-19-surveillance'

    output_cols = [
        'Date', 'Level_Type', 'Level',
        'new_cases_probable_plus_confirmed', 'new_cases_confirmed', 'new_cases_probable',
        'cumulative_cases_probable_plus_confirmed', 'cumulative_cases_confirmed', 'cumulative_cases_probable',
        'new_deaths',
        'new_hospitalizations', 'hospitalizations_7_day',
    ]

    raw_table = get_result_table(base_url=dshs_base_url)
    report_date = get_report_date(base_url=dshs_base_url)

    # endregion

    # region clean --------------------------------------------------------------------------------
    fixed_names = fix_table_names(raw_table)
    cleaned_table = apply_metric_fixes(fixed_names)
    reshaped_df = reshape_df(cleaned_table)
    final_df = clean_pivoted_df(reshaped_df, report_date, output_cols)
    return final_df
# endregion
