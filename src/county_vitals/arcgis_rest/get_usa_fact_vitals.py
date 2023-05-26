import pandas as pd
import requests
import io


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


config = {
    "cases":
        {
            "vital_type": "cases",
            "url": "https://static.usafacts.org/public/data/covid-19/covid_confirmed_usafacts.csv",
            "cookies": {
                '_dd_s': 'rum=0&expire=1685106111009',
            },
            "headers": {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                # 'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                # 'Cookie': '_dd_s=rum=0&expire=1685106111009',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-site',
                'Sec-Fetch-User': '?1',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
            },
        },
    "deaths":
        {
            "vital_type": "deaths",
            "url": "https://static.usafacts.org/public/data/covid-19/covid_deaths_usafacts.csv",
            "cookies": {
                '_dd_s': 'rum=0&expire=1685106685662',
            },
            "headers": {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                # 'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                # 'Cookie': '_dd_s=rum=0&expire=1685106685662',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-site',
                'Sec-Fetch-User': '?1',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
            }
        }

}
results = [
    get_vitals(config['cases']),
    get_vitals(config['deaths'])
]

# add deaths too

final_df = (
    pd.merge(results[0], results[1], on=['date', 'county'])
    .assign(cases_daily=lambda x: x.groupby('county')['cases_cumulative'].diff())
    .assign(deaths_daily=lambda x: x.groupby('county')['deaths_cumulative'].diff())
    .assign(source='usafacts.org')
)

final_df.to_csv('tableau/vitals/staging/other_sources/usa_facts.csv', index=False)

