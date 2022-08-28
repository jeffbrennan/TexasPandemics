import time
from datetime import datetime, timedelta
from dateutil.parser import parse as parsedate
from requests import get
import re
import pandas as pd
from subprocess import Popen
import sys


def run_bat(bat_file):
    p = Popen(bat_file)
    stdout, stderr = p.communicate()


def parse_file(file_url, header_loc):
    df = pd.ExcelFile(file_url, engine='openpyxl').parse(
        sheet_name=0, header=header_loc)

    date_text = list(df.columns)[-1]
    date_matches = re.findall(r'(\d{1,2}\/\d{1,2}\/\d{4})', date_text)
    max_date = parsedate(date_matches[-1])
    return 1 if max_date == TODAY.date() else 0


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


def run_daily():
    print('\nChecking dashboard files...')
    check_update(UPDATE_URL, max_attempts=100)
    print('\nDashboard files are ready!')
    run_bat(TMC_BAT)
    run_bat(DAILY_BAT)


# data updates at ~ 5PM EST
# if running after midnight (4 UTC) or before noon (16 UTC), subtract 1 day
if datetime.utcnow().hour > 4 and datetime.utcnow().hour < 16:
    TODAY = datetime.now() - timedelta(days=1)
else:
    TODAY = datetime.now()

TMC_BAT = r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\requests_auto.bat'
DAILY_BAT = r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\scrape.bat'
UPDATE_URL = [['https://dshs.texas.gov/coronavirus/TexasCOVID19CaseCountData.xlsx', 0]]
TODAY_INT = TODAY.weekday()
if TODAY_INT < 5:
    run_daily()
