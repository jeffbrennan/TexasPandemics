import os

import pandas as pd
from dotenv import load_dotenv
from sodapy import Socrata

import src.utils


def create_client() -> Socrata:
    load_dotenv('.env')
    client = Socrata(
        domain="data.cdc.gov",
        app_token=os.environ.get('CDC_WW_TOKEN')
    )
    return client


def get_data(client: Socrata, offset: int, dataset_id: str, current_max_date: str) -> list[dict]:
    results = client.get(
        dataset_identifier=dataset_id,
        select="county_names, sample_location, key_plot_id, population_served, first_sample_date, date_start, date_end, ptc_15d, percentile, detect_prop_15d",
        where=f"reporting_jurisdiction = 'Texas' and date_start > '{current_max_date}'",
        order="date_start",
        limit=1000,
        offset=offset
    )
    return results


def format_raw_data(df: list) -> pd.DataFrame:
    formatted_df = (
        df
        .rename(
            columns={
                'county_names': 'County',
                'date_end': 'Date',
                'ptc_15d': 'normalized_levels_pct_difference_15d',
                'detect_prop_15d': 'pct_samples_with_detectable_levels_15d',
                'percentile': 'normalized_levels_15d'
            }
        )
        .drop(columns=['date_start'])
        .assign(Date=lambda x: pd.to_datetime(x['Date']).dt.date)
        .astype(
            {
                'population_served': 'int32',
                'normalized_levels_pct_difference_15d': 'float32',
                'pct_samples_with_detectable_levels_15d': 'float32',
                'normalized_levels_15d': 'float32'
            }
        )
    )

    formatted_df[['County1', 'County2']] = formatted_df['County'].str.split(',', n=1, expand=True)

    formatted_df = (
        formatted_df
        .drop(columns=['County'])
        .rename(columns={'County1': 'County'})
        [[
            'County', 'County2', 'Date',
            'sample_location', 'key_plot_id', 'population_served',
            'normalized_levels_15d',
            'normalized_levels_pct_difference_15d',
            'pct_samples_with_detectable_levels_15d',
        ]]
    )

    return formatted_df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    formatted_df = format_raw_data(df)

    clean_df = (
        formatted_df
        .dropna(
            subset=
            [
                'normalized_levels_15d',
                'normalized_levels_pct_difference_15d',
                'pct_samples_with_detectable_levels_15d'
            ]
            , how='any'
        )
        .sort_values(by=['County', 'Date'])
        .drop_duplicates(subset=['County', 'Date', 'key_plot_id'], keep='first')
    )

    return clean_df


def write_raw_results(df: pd.DataFrame) -> None:
    write_date = df.date_end.max()
    src.utils.write_file(df, f'original-sources/historical/wastewater/cdc/cdc_wastewater_raw_{write_date}')


def create_offsets(client: object, dataset_id: str, current_max_date: str) -> list[int]:
    def get_max_date(client: object, dataset_id: str) -> str:
        response = client.get(
            dataset_identifier=dataset_id,
            select="MAX(date_end) as max_date",
            where=f"reporting_jurisdiction = 'Texas'"
        )
        output = response[0]['max_date']
        return output

    def get_num_records(client: object, dataset_id: str):
        response = client.get(
            dataset_identifier=dataset_id,
            select="COUNT(*) as n ",
            where=f"reporting_jurisdiction = 'Texas' and date_end > '{current_max_date}'"
        )

        output = int(response[0]['n'])
        return output

    live_max_date = get_max_date(client, dataset_id)
    num_records = get_num_records(client, dataset_id)
    assert num_records > 0, f'No new records found, max date available is {live_max_date}'

    offsets = [i for i in range(0, num_records, 1000)]
    return offsets


def run_diagnostics(df: pd.DataFrame) -> None:
    check_duplicate_values = (
        df
        .groupby(['County', 'Date', 'key_plot_id'])
        .size()
        .reset_index()
        .rename(columns={0: 'count'})
        .query('count > 1')
    )

    assert check_duplicate_values.shape[
               0] == 0, f'Found {check_duplicate_values.shape[0]} duplicate county/date/plant pairs'

    check_county_not_null = df[df['County'].isnull()]
    assert check_county_not_null.shape[0] == 0, f'Found {check_county_not_null.shape[0]} null counties'

    check_date_not_null = df[df['Date'].isnull()]
    assert check_date_not_null.shape[0] == 0, f'Found {check_date_not_null.shape[0]} null dates'


# region setup --------------------------------------------------------------------------------

def main():
    client = create_client()

    DATASET_ID = "2ew6-ywp6"

    current_df = src.utils.load_csv('tableau/wastewater/cdc_wastewater.csv')
    current_max_date = current_df['Date'].max()

    # region pull data --------------------------------------------------------------------------------
    offsets = create_offsets(client, DATASET_ID, current_max_date)
    results = [get_data(client, offset, DATASET_ID, current_max_date) for offset in offsets]
    results_df = pd.concat([pd.DataFrame.from_records(i) for i in results])
    write_raw_results(results_df)
    # endregion

    # region clean_data --------------------------------------------------------------------------------
    clean_df = clean_data(results_df)
    clean_df_out = pd.concat([current_df, clean_df]).drop_duplicates()
    # endregion

    # region diagnostics + upload --------------------------------------------------------------------------------
    run_diagnostics(clean_df_out)
    src.utils.write_file(clean_df_out, 'tableau/wastewater/cdc_wastewater')


if __name__ == '__main__':
    main()
# endregion
