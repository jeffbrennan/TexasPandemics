from datetime import date, datetime as dt
import pandas as pd

import src.utils


def obtain_urls(num_reports: int = 1) -> dict:
    url_prefix = 'https://d1t7q96h7r5kqm.cloudfront.net/'
    url_suffix = '_automated_csvs/wastewater_by_county.csv'

    start_date = date(2020, 3, 5)
    end_date = dt.today().date()
    expected_dates = pd.date_range(start_date, end_date, freq='W-THU').strftime('%Y-%m-%d').tolist()
    url_list = [f'{url_prefix}{expected_date}{url_suffix}' for expected_date in expected_dates[-num_reports:]]

    output = {
        'urls': url_list,
        'expected_dates': expected_dates[-num_reports:]
    }

    return output


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    clean_df = (
        df
        .query('state_abbr == "TX"')
        .rename(
            columns={
                'display_name': 'County',
                'date': 'Date',
                'eff_conc_sarscov2_weekly': 'viral_copies_per_ml',
                'eff_conc_sarscov2_weekly_rolling': 'viral_copies_per_ml_rolling'
            }
        )
        [['County', 'Date', 'viral_copies_per_ml', 'viral_copies_per_ml_rolling']]
        .assign(County=lambda x: x.County.str.replace(' County, TX', ''))
        .sort_values(['County', 'Date'])
    )

    return clean_df


def run_diagnostics(df: pd.DataFrame) -> None:
    def check_not_null(df: pd.DataFrame, col: str) -> bool:
        return df[col].isnull().any() == False

    col_not_null = ['County', 'Date']
    check_col_not_null = all([check_not_null(df, col) for col in col_not_null])

    checks = [check_col_not_null]
    assert checks


def check_if_new(new_df: pd.DataFrame, current_df: pd.DataFrame) -> None:
    raw_data_max_date = new_df['date'].max()
    current_data_max_date = current_df['Date'].max()

    file_is_new = raw_data_max_date > current_data_max_date
    assert file_is_new


# region setup --------------------------------------------------------------------------------

def main():
    raw_output_base_path = 'original-sources/historical/wastewater/biobot/biobot_wastewater_raw'
    current_df = src.utils.load_csv('tableau/wastewater/biobot_wastewater.csv')

    wastewater_run_data = obtain_urls(1)
    raw_data = [src.utils.load_csv(i) for i in wastewater_run_data['urls']]

    for i, ww_df in enumerate(raw_data):
        check_if_new(ww_df, current_df)

        src.utils.write_file(
            df=ww_df,
            table_path=f'{raw_output_base_path}_{wastewater_run_data["expected_dates"][i]}'
        )

    cleaned_biobot_data = pd.concat([clean_data(i) for i in raw_data]).drop_duplicates()
    run_diagnostics(cleaned_biobot_data)

    src.utils.write_file(cleaned_biobot_data, 'tableau/wastewater/biobot_wastewater')


# endregion

if __name__ == '__main__':
    main()
