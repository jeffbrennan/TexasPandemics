import pandas as pd
import requests
from bs4 import BeautifulSoup
import regex as re

dshs_base_url = 'https://www.dshs.texas.gov/covid-19-coronavirus-disease/texas-covid-19-surveillance'
from datetime import datetime as dt
from datetime import timedelta


def get_result_table(base_url: str) -> pd.DataFrame:
    dshs_table = pd.read_html(base_url)
    raw_data_orig = dshs_table[0]

    # remove footnote
    raw_data = raw_data_orig[raw_data_orig.iloc[:, 0].str.contains("provisional") == False]

    # check if columns contain string surveillance component and last week
    # TODO: update with full col substr list
    check_col_names = any(['Surveillance' in col for col in raw_data.columns])
    assert check_col_names

    return raw_data


def fix_table_names(raw_df: pd.DataFrame) -> pd.DataFrame:
    orig_colnames = raw_df.columns.to_list()
    fixed_colnames = [i.lower().strip().replace('\xa0', ' ') for i in orig_colnames]
    raw_df.columns = fixed_colnames

    cleaned_df = (
        raw_df
        .rename(columns={
            'texas surveillance component': 'metric',
            'change from previous week': 'last_week_delta',
            'current week': 'this_week',
            'previous week': 'last_week'
        }
        )
    )
    return cleaned_df


def get_report_date(base_url: str) -> str:
    page_response = requests.get(base_url)
    soup = BeautifulSoup(page_response.text, "html.parser")
    report_date: str = soup.find('b', text=re.compile('\d{1,2}/\d{1,2}/\d{4}')).text
    # return data formatted as yyyy-mm-dd
    parsed_date = dt.strptime(report_date, '%m/%d/%Y').date()

    # apply parsed date checks
    min_date_delta = 7
    current_date = dt.now().date()
    min_date = current_date - timedelta(days=min_date_delta)

    date_is_less_than_or_equal_to_current = parsed_date <= current_date
    date_is_greater_or_equal_to_delta = parsed_date >= min_date

    assert date_is_less_than_or_equal_to_current
    assert date_is_greater_or_equal_to_delta

    # return
    formatted_date = dt.strftime(parsed_date, '%Y-%m-%d')
    return formatted_date


get_report_date(dshs_base_url)


def fix_metric(metric: str) -> str:
    metric_map = {
        'new covid-19 cases (probable and confirmed)': 'new_cases_probable_plus_confirmed',
        'new covid-19 confirmed cases': 'new_cases_confirmed',
        'new covid-19 probable cases': 'new_cases_probable',
        'total covid-19 cases (probable and confirmed)': 'cumulative_cases_probable_plus_confirmed',
        'total covid-19 confirmed cases': 'cumulative_cases_confirmed',
        'total covid-19 probable cases': 'cumulative_cases_probable',
        'newly reported covid-19-associated fatalities': 'new_deaths',
        'hospitalized covid-19 cases (day of report)': 'new_hospitalizations',
        'hospitalized covid-19 cases (rolling 7 day average)': 'hospitalizations_7_day'
    }
    cleaned_metric = metric.lower().strip().replace('*', '').replace('\xa0', ' ')
    metric_out = metric_map[cleaned_metric]
    return metric_out


def apply_metric_fixes(raw_df: pd.DataFrame) -> pd.DataFrame:
    cleaned_metric = [fix_metric(i) for i in raw_df['metric']]
    cleaned_df = (
        raw_df
        .rename({'metric': 'metric_orig'})
        .assign(metric=cleaned_metric)
    )
    return cleaned_df


def reshape_df(cleaned_df: pd.DataFrame) -> pd.DataFrame:
    # pivot long to wide
    reshaped_df = (
        cleaned_df
        [['metric', 'this_week']]
        .assign(tempindex=1)
        .pivot(index='tempindex', columns='metric', values='this_week')
        .reset_index()
        .drop(['tempindex'], axis=1)
    )
    return reshaped_df


def write_update(final_df: pd.DataFrame) -> None:
    final_df.to_csv('tableau/state_vitals.csv', index=False)


def clean_pivoted_df(reshaped_df: pd.DataFrame, report_date: str) -> pd.DataFrame:
    final_df = (
        reshaped_df
        .replace('▼|▲|\s', '', regex=True)
        .astype('int32')
        .assign(Date=report_date)
        .assign(Level_Type='State')
        .assign(Level='Texas')

        [['Date', 'Level_Type', 'Level',
          'new_cases_probable_plus_confirmed', 'new_cases_confirmed', 'new_cases_probable',
          'cumulative_cases_probable_plus_confirmed', 'cumulative_cases_confirmed', 'cumulative_cases_probable',
          'new_deaths',
          'new_hospitalizations', 'hospitalizations_7_day',
          ]]
    )
    return final_df


def run_diagnostics(final_df: pd.DataFrame) -> None:
    null_vals_exist = final_df.isnull().values.any()
    assert not (null_vals_exist)


raw_table = get_result_table(base_url=dshs_base_url)
report_date = get_report_date(base_url=dshs_base_url)
fixed_names = fix_table_names(raw_table)
cleaned_table = apply_metric_fixes(fixed_names)
reshaped_df = reshape_df(cleaned_table)
final_df = clean_pivoted_df(reshaped_df, report_date)

run_diagnostics(final_df)

write_update(final_df)
