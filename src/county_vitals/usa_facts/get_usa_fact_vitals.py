import pandas as pd
import requests
import io
import yaml
from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import datetime as dt


def clean_vitals(df: pd.DataFrame) -> pd.DataFrame:
    load_dotenv()
    dshs_max_date = os.getenv("DSHS_END_COUNTY_LEVEL_REPORTING_DATE")
    clean_df = (
        df
        .query(f"date > '{dshs_max_date}'")
        .rename(
            columns={
                'date': 'Date',
                'county': 'County'
            }
        )
        .assign(Date=lambda x: pd.to_datetime(x.Date).dt.date)
    )

    return clean_df


def get_vitals(config: dict) -> pd.DataFrame:
    response = requests.get(
        config['url'],
        cookies=config['cookies'],
        headers=config['headers']
    )

    raw_data = response.content
    raw_df = pd.read_csv(io.StringIO(raw_data.decode('utf-8')))

    clean_df = (
        raw_df
        .query('State == "TX"')
        .drop(columns=['countyFIPS', 'StateFIPS', 'State'])
        .rename(columns={'County Name': 'county'})
        .set_index('county')
        .T
        .reset_index()
        .rename(columns={'index': 'date'})
        .melt(id_vars=['date'], var_name='county', value_name=f'{config["vital_type"]}_cumulative')
        .assign(county=lambda x: x['county'].str.replace(' County', ''))
        .assign(date=lambda x: pd.to_datetime(x['date']))
        .sort_values(['county', 'date'])
        .reset_index(drop=True)
    )
    return clean_df


def combine_vitals(results: list) -> pd.DataFrame:
    final_df = (
        pd.merge(results[0], results[1], on=['date', 'county'])
        .assign(cases_daily=lambda x: x.groupby('county')['cases_cumulative'].diff())
        .assign(deaths_daily=lambda x: x.groupby('county')['deaths_cumulative'].diff())
        .assign(source='usafacts.org')
    )
    return final_df


def main() -> None:
    config = yaml.safe_load(Path('usa_facts_config.yaml').read_text())
    results = [
        get_vitals(config['cases']),
        get_vitals(config['deaths'])
    ]
    final_df = combine_vitals(results)
    # final_df.to_csv('data/usa_fact_vitals.csv', index=False)


if __name__ == '__main__':
    main()
