import time
from datetime import datetime, timedelta
from dateutil.parser import parse as parsedate
from requests import get
import re
from bs4 import BeautifulSoup
import pandas as pd
from subprocess import Popen


def run_bat(bat_file): 
  p = Popen(bat_file)
  stdout, stderr = p.communicate()


def parse_file(file_url, header_loc): 
  # match 2/4 digits, separator, 2/4 digits, optional separator, optional 2/4 digits
  date_regex = r'((\d{2}|\d{4})(\.|\-|\/)(\d{2}|\d{4})?(\.|\-|\/)?(\d{2}|\d{4}))'

  if file_url == 'https://dshs.texas.gov/coronavirus/TexasCOVID19Demographics.xlsx.asp': 
    r = get('https://dshs.texas.gov/coronavirus/additionaldata/')
    soup = BeautifulSoup(r.text, 'lxml')
    parent = soup.find("a", {"title": "Case and Fatality Demographics Data "})
    date_text = parent.nextSibling.nextSibling.text
    max_date = parsedate(re.search(date_regex, date_text).group(0))

  else:
    df = pd.ExcelFile(file_url, engine='xlrd').parse(sheet_name=0, header=header_loc)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    date_text = list(df.columns)[-1]
    max_date = parsedate(re.search(date_regex, date_text).group(0)).date()
  return 1 if max_date == today.date() else 0


def check_update(files): 
  new_files = 0
  num_files = len(files)
  attempts = 0

  while new_files < num_files: 
    for file in files:
      print(f'\nAttempt #{attempts+1}\n{file[0]}')
      new_file = parse_file(file[0], file[1])

      if new_file == 1:
        print('New file!')
        new_files +=1
      else:
        print('File not updated yet. Retrying in 5 minutes...')
        time.sleep(300)
        count+=1


# if thursday (3), add schools, if friday (4), add demo, else none
def weekly_updates(today):
  day = today.weekday()
  ymd_date = today.strftime('%Y%m%d')
  return { 
    3: [f'https://dshs.texas.gov/chs/data/tea/district-level-school-covid-19-case-data/district-level-data-file_{ymd_date}.xls', 3],
    4: ['https://dshs.texas.gov/coronavirus/TexasCOVID19Demographics.xlsx.asp', 3]
  }.get(day, [])


def run_tmc(): 
  tmc_bat = r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\TMC.bat'
  tmc_url = [['https://dshs.texas.gov/coronavirus/TexasCOVID-19NewCasesOverTimebyCounty.xlsx', 2]]

  print('Checking new cases...')
  check_update(tmc_url)
  run_bat(tmc_bat)
  print('\nNew cases are ready!')


def run_daily():
  daily_bat = [r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\scrape.bat']
  daily_url = [['https://dshs.texas.gov/coronavirus/TexasCOVID19CaseCountData.xlsx', 0],
              ['https://dshs.texas.gov/coronavirus/CombinedHospitalDataoverTimebyTSA.xlsx', 2]]
  daily_url.extend(weekly_updates(today.date()))

  print('\nChecking dashboard files...')
  check_update(daily_url)
  print('\nDashboard files are ready!')
  run_bat(daily_bat)

# set day as current day if after 5PM EST (10PM UTC), else subtract 1 day
today = datetime.now() if datetime.utcnow().hour >= 20 else datetime.now() - timedelta(days=1)

run_tmc()
run_daily()