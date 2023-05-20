import pandas as pd
import matplotlib.pyplot as plt

required_data_points = {
    'new_cases_probable_plus_confirmed': 'county',
    'new_cases_confirmed': 'county',
    'new_cases_probable': 'county',
    'cumulative_cases_probable_plus_confirmed': 'county',
    'cumulative_cases_confirmed': 'county',
    'cumulative_cases_probable': 'county',
    'new_deaths': 'county',
    'new_hospitalizations': 'hospitalizations_tsa',
    'hospitalizations_7_day': 'hospitalizations_tsa'
}

base_url = 'https://raw.githubusercontent.com/jeffbrennan/TexasPandemics/main/tableau'
# region pull -----
county_raw = pd.read_csv(f'{base_url}/county.csv')
hosp_raw = pd.read_csv(f'{base_url}/hospitalizations_tsa.csv')
# endregion

# region county --------------------------------------------------------------------------------
# region probable --------------------------------------------------------------------------------
county_list = county_raw['County'].unique().tolist()

def get_file_as_df(url: str, sheet_index: int) -> pd.DataFrame:
    df = pd.read_excel(url, sheet_name=sheet_index, skiprows=2)
    return df


def manage_vitals(url: str, sheet_index: int, vital_type: str) -> pd.DataFrame:
    print(sheet_index)
    raw_df = get_file_as_df(url, sheet_index)
    pivoted_df = raw_df.melt(id_vars=['County'], var_name='Date', value_name=vital_type)

    test = pivoted_df['Date']
    test.str.contains('Total', na=False)

    pivoted_df_clean = (
        pivoted_df[pivoted_df['County'].isin(county_list)]
        .query("Date != 'Unknown Date'")
        .query("Date.str.contains('^Total', na=False) == False")
        .assign(Date=lambda x: pd.to_datetime(x['Date']).dt.date)

    )
    return pivoted_df_clean


def clean_new_vitals(df, vital_type):
    new_df = (
        df
        .rename(columns={f'{vital_type}': f'new_{vital_type}'})
        .groupby(['Date'])
        .agg({f'new_{vital_type}': 'sum'})
        .reset_index()
        .assign(Date=lambda x: pd.to_datetime(x['Date']))
        .resample('W', on='Date')[f'new_{vital_type}']
        .sum()
        .reset_index()
    )

    cumulative_df = (
        new_df[f'new_{vital_type}'].cumsum()
        .rename(f'cumulative_{vital_type}')
    )

    combined_df = (new_df.join(cumulative_df, how='left'))

    return combined_df


def clean_cumulative_vitals(df, vital_type):
    cumulative_df = (
        df
        .rename(columns={f'{vital_type}': f'cumulative_{vital_type}'})
        .groupby(['Date'])
        .agg({f'cumulative_{vital_type}': 'sum'})
        .reset_index()
        .assign(Date=lambda x: pd.to_datetime(x['Date']))
        .resample('W', on='Date')[f'cumulative_{vital_type}']
        .max()
        .reset_index()
    )

    #     difference by lag date
    new_df = (
        cumulative_df
        .set_index('Date')
        .diff()
        .rename(columns={f'cumulative_{vital_type}': f'new_{vital_type}'})
        .reset_index()
    )
    combined_df = (cumulative_df.merge(new_df, how='left', left_on='Date', right_on='Date'))

    return combined_df


# region probable --------------------------------------------------------------------------------


probable_url = 'https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20New%20Probable%20Cases%20by%20County.xlsx'
county_probable_combined = pd.concat(
    [manage_vitals(probable_url, i, 'cases_probable') for i in range(0, 4)]
)

probable_cases = clean_new_vitals(county_probable_combined, 'cases_probable')
# endregion


# region confirmed --------------------------------------------------------------------------------
confirmed_cases_url = 'https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20New%20Confirmed%20Cases%20by%20County.xlsx'
county_confirmed_combined = pd.concat(
    [manage_vitals(confirmed_cases_url, i, 'cases_confirmed') for i in range(0, 4)]
)

confirmed_cases = clean_new_vitals(county_confirmed_combined, 'cases_confirmed')

# endregion

# region deaths --------------------------------------------------------------------------------
deaths_url = 'https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20Fatality%20Count%20Data%20by%20County.xlsx'
county_deaths_combined = pd.concat(
    [manage_vitals(deaths_url, i, 'deaths') for i in range(0, 4)]
)

deaths = clean_cumulative_vitals(county_deaths_combined, 'deaths')
# endregion

# region get new vals --------------------------------------------------------------------------------

new_vals = pd.read_csv('tableau/state_vitals.csv')
new_vals_parsed = (
    new_vals
    .assign(Date=lambda x: pd.to_datetime(x['Date']))
)

# endregion
# endregion


# region hosp --------------------------------------------------------------------------------
hosp_new = (
    hosp_raw
    [['Date', 'Hospitalizations_24']]
    .groupby('Date')
    .agg({'Hospitalizations_24': 'sum'})
    .reset_index()
    .assign(Date=lambda x: pd.to_datetime(x['Date']))
    .resample('W', on='Date')['Hospitalizations_24']
    .sum()
    .reset_index()
    .rename(columns={'Hospitalizations_24': 'new_hospitalizations'})
)

hosp_7_day = (
    hosp_raw
    [['Date', 'Hospitalizations_Total']]
    .groupby('Date')
    .agg({'Hospitalizations_Total': 'sum'})
    .reset_index()
    .assign(Date=lambda x: pd.to_datetime(x['Date']))
    .resample('W', on='Date')['Hospitalizations_Total']
    .mean()
    .reset_index()
    .rename(columns={'Hospitalizations_Total': 'hospitalizations_7_day'})
)

hosp_combined = (
    hosp_new
    .merge(hosp_7_day, how='left')
)

# endregion


# region combine  --------------------------------------------------------------------------------
output_cols = ['Date', 'Level_Type', 'Level',
               'new_cases_probable_plus_confirmed', 'new_cases_confirmed', 'new_cases_probable',
               'cumulative_cases_probable_plus_confirmed', 'cumulative_cases_confirmed', 'cumulative_cases_probable',
               'new_deaths',
               'new_hospitalizations', 'hospitalizations_7_day',
               ]

vitals_combined = (
    confirmed_cases
    .merge(probable_cases, how='left')
    .merge(deaths, how='left')
    .merge(hosp_combined, how='left')
    .assign(
        new_cases_probable_plus_confirmed=lambda x: x['new_cases_confirmed'].add(x['new_cases_probable'], fill_value=0))
    .assign(cumulative_cases_probable_plus_confirmed=lambda x: x['cumulative_cases_confirmed'].add(
        x['cumulative_cases_probable'], fill_value=0))
    .assign(
        Level_Type='State',
        Level='Texas')
    [output_cols]
)

vitals_combined_historic = (
    vitals_combined
    .query('Date < "2023-05-09"')
)

output_df = (
    pd.concat([vitals_combined_historic,
               new_vals_parsed])
    .sort_values('Date')
    .convert_dtypes()
)
# region diagnostics --------------------------------------------------------------------------------
# create plot of all metrics by date

# cumulative values show big drop
output_df.plot(x='Date', y=output_cols[3:], subplots=True, layout=(4, 3), figsize=(15, 10))
plt.show()


# check no cumulative value decrease


def check_cumulative_decrease(df: pd.DataFrame, col: str) -> bool:
    return df[col].diff().le(0).any()


# display as column name list
cumulative_cols = ['cumulative_cases_probable_plus_confirmed', 'cumulative_cases_confirmed',
                   'cumulative_cases_probable']
cumulative_cols_decrease = any([check_cumulative_decrease(output_df, col) for col in cumulative_cols])
print(f'ERROR: cumulative_cols_decrease: {cumulative_cols_decrease}')
assert cumulative_cols_decrease is False
# endregion
# region upload --------------------------------------------------------------------------------

output_df.to_csv('tableau/state_vitals.csv', index=False)

# endregion
