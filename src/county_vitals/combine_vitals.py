import pandas as pd
import glob
from src.utils import write_file, load_csv


# get list of all county files


def list_files() -> list:
    vitals_dir = 'tableau/vitals/staging/'
    return glob.glob(f'{vitals_dir}/*_vitals.csv')


def load_files(file_list: list) -> pd.DataFrame:
    return pd.concat([load_csv(f) for f in file_list], axis=0)


def combine_vitals(df: pd.DataFrame) -> pd.DataFrame:
    def handle_cumulative(df: pd.DataFrame) -> pd.DataFrame:
        cumulative_counties = (
            df
            [['County', 'Date', 'cases_cumulative', 'deaths_cumulative']]
            .replace(0, pd.NA)
            .dropna()
            .assign(cases_daily=lambda x: x.groupby('County')['cases_cumulative'].diff())
            .assign(deaths_daily=lambda x: x.groupby('County')['deaths_cumulative'].diff())
        )
        return cumulative_counties

    def handle_daily(df: pd.DataFrame) -> pd.DataFrame:
        daily_counties = (
            df
            [['County', 'Date', 'cases_daily', 'deaths_daily']]
            .dropna()
            .assign(cases_cumulative=lambda x: x.groupby('County')['cases_daily'].cumsum())
            .assign(deaths_cumulative=lambda x: x.groupby('County')['deaths_daily'].cumsum())
        )
        return daily_counties

    cumulative_counties = handle_cumulative(df)
    daily_counties = handle_daily(df)
    county_files_combined = pd.concat([cumulative_counties, daily_counties], axis=0)
    return county_files_combined


def clean_vitals(df: pd.DataFrame) -> pd.DataFrame:
    county_files_clean = (
        df
        .sort_values(['County', 'Date'])
        .astype(
            {
                'cases_cumulative': 'Int32[pyarrow]',
                'deaths_cumulative': 'Int32[pyarrow]',
                'cases_daily': 'Int32[pyarrow]',
                'deaths_daily': 'Int32[pyarrow]'
            }
        )
    )
    return county_files_clean


def run_diagnostics(df: pd.DataFrame) -> None:
    # cumulative counts monotonic increase
    # no negatives in daily or cumulative counts
    pass


def main():
    county_files = list_files()
    county_files_combined_raw = load_files(county_files)
    county_files_combined = combine_vitals(county_files_combined_raw)
    county_files_clean = clean_vitals(county_files_combined)

    run_diagnostics(county_files_clean)
    write_file(county_files_clean, 'tableau/vitals/county_vitals')


if __name__ == '__main__':
    main()
