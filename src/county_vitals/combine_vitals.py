import pandas as pd
import glob
from src.utils import write_file, load_parquet


def list_files() -> list:
    vitals_dir = 'data/origin/vitals/'
    return glob.glob(f'{vitals_dir}/*_vitals.parquet')


def load_files(file_list: list) -> pd.DataFrame:
    return pd.concat([load_parquet(f) for f in file_list], axis=0)


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
    def replace_cumulative_vals(row: pd.Series, column: str) -> pd.Series:
        if pd.isna(row['diff']):
            return row[column]
        elif row['diff'] < 0:
            return row['shifted']
        else:
            return row[column]

    def clean_vitals_prep(df: pd.DataFrame) -> pd.DataFrame:
        county_vitals_prep = (
            df
            .assign(Date=lambda x: pd.to_datetime(x['Date']).dt.date)
            .assign(cases_daily=lambda x: x['cases_daily'].clip(lower=0))
            .assign(cases_daily=lambda x: x['cases_daily'].fillna(0))
            .assign(deaths_daily=lambda x: x['deaths_daily'].clip(lower=0))
            .assign(deaths_daily=lambda x: x['deaths_daily'].fillna(0))
            .sort_values(['County', 'Date'])
        )
        return county_vitals_prep

    def clean_vitals_monotonic(df: pd.DataFrame, column) -> pd.DataFrame:
        county_vitals_ensure_monotonic = (
            df
            .groupby('County', group_keys=True)
            .apply(lambda group: group
                   .assign(diff=lambda x: x[column].diff())
                   .assign(shifted=lambda x: x[column].shift())
                   .assign(**{column: lambda x: x.apply(lambda row: replace_cumulative_vals(row, column), axis=1)})
                   .drop(['diff', 'shifted'], axis=1))
            .reset_index(drop=True)
        )
        return county_vitals_ensure_monotonic

    county_vitals_prep = clean_vitals_prep(df)

    county_vitals_ensure_monotonic_cases = clean_vitals_monotonic(
        county_vitals_prep,
        'cases_cumulative'
    )
    county_vitals_ensure_monotonic_deaths = clean_vitals_monotonic(
        county_vitals_ensure_monotonic_cases,
        'deaths_cumulative'
    )

    county_vitals_clean = (
        county_vitals_ensure_monotonic_deaths
        .astype(
            {
                'cases_cumulative': 'Int32',
                'cases_daily': 'Int32',
                'deaths_cumulative': 'Int32',
                'deaths_daily': 'Int32'
            }
        )
        .assign(source='county level dashboards')
        .assign(Date=lambda x: pd.to_datetime(x['Date']).dt.date)
    )
    return county_vitals_clean


def main():
    county_files = list_files()
    county_files_combined_raw = load_files(county_files)
    county_files_combined = combine_vitals(county_files_combined_raw)
    county_files_clean = clean_vitals(county_files_combined)

    write_file(county_files_clean, 'tableau/vitals/county_vitals')


if __name__ == '__main__':
    main()
