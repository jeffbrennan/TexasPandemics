import time
from datetime import datetime, timedelta
from dateutil.parser import parse as parsedate
from requests import get
import re
from bs4 import BeautifulSoup
import pandas as pd
from subprocess import Popen
import sys


def run_bat(bat_file):
    p = Popen(bat_file)
    stdout, stderr = p.communicate()


def parse_file(file_url, header_loc):
    # match 2/4 digits, separator, 2/4 digits, optional separator, optional 2/4 digits
    date_regex = r'((\d{4}|\d{2}|\d{1})(\.|\-|\/)(\d{4}|\d{2}|\d{1})?(\.|\-|\/)?(\d{4}|\d{2}))'

    if 'Demographics' in file_url:
        r = get('https://dshs.texas.gov/coronavirus/additionaldata/')
        soup = BeautifulSoup(r.text, 'lxml')
        parent = soup.find(
            "a", {"title": "Case and Fatality Demographics Data "})
        date_text = parent.nextSibling.nextSibling.text
        max_date = parsedate(re.search(date_regex, date_text).group(0))

    elif 'district-level' in file_url:
        # url updates weekly, if pandas can read and rows are approx expected, then file is updated
        try:
            df = pd.ExcelFile(file_url, engine='xlrd').parse(
                sheet_name=0, header=header_loc)
            if len(df.index) > 1000:
                max_date = today.date()
        except:
            pass
    else:
        df = pd.ExcelFile(file_url, engine='xlrd').parse(
            sheet_name=0, header=header_loc)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        date_text = list(df.columns)[-1]
        max_date = parsedate(re.search(date_regex, date_text).group(0)).date()
    return 1 if max_date == today.date() else 0


def check_update(files, max_attempts, check_interval=600):
    for file in files:
        attempts = 1
        new_file = 0
        print(file[0])

        while new_file == 0:
            new_file = parse_file(file[0], file[1])

            if new_file == 1:
                print(f'Attempt #{attempts} | New file!')
                break
            elif attempts == max_attempts:
                sys.exit(
                    'Too many attempts. Manually check https://dshs.texas.gov/coronavirus/additionaldata/')
            else:
                print(
                    f'Attempt #{attempts} | File not updated yet. Retrying in {check_interval // 60} minutes...')
                time.sleep(check_interval)
                attempts += 1


# if thursday (3), add schools, if friday (4), add demo, else none
def weekly_updates(today):
    day = today.weekday()
    district_date = (today - timedelta(days=2)).strftime('%m%d%Y')
    return {
        4: [['https://dshs.texas.gov/coronavirus/TexasCOVID19Demographics.xlsx.asp', 3],
            [f'https://dshs.texas.gov/chs/data/tea/district-level-school-covid-19-case-data/district-level-data-file_{district_date}.xls', 0]
            ]
    }.get(day, [])


def run_requests():
    tmc_bat = r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\requests.bat'
    tmc_url = [
        ['https://dshs.texas.gov/coronavirus/TexasCOVID-19NewCasesOverTimebyCounty.xlsx', 2]]

    print('Checking new cases...')
    check_update(tmc_url, max_attempts=30)
    run_bat(tmc_bat)
    print('\nNew cases are ready!')


def run_daily():
    daily_bat = [
        r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\scrape.bat']
    daily_url = [['https://dshs.texas.gov/coronavirus/TexasCOVID19CaseCountData.xlsx', 0]]
    daily_url.extend(weekly_updates(today.date()))

    print('\nChecking dashboard files...')
    check_update(daily_url, max_attempts=6)
    print('\nDashboard files are ready!')
    run_bat(daily_bat)


# data updates at ~ 5PM EST
# if running after midnight (4 UTC) or before noon (16 UTC), subtract 1 day
if datetime.utcnow().hour > 4 and datetime.utcnow().hour < 16:
    today = datetime.now() - timedelta(days=1)
else:
    today = datetime.now()

run_requests()
run_daily()