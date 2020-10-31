import time
import pandas as pd
from requests import get
from datetime import datetime
from dateutil.parser import parse as parsedate
from subprocess import Popen


def check_file(): 
  df = pd.ExcelFile(file_url, engine='xlrd').parse(sheet_name=0, header=2)
  max_date = parsedate(list(df.columns)[-1].replace('New Cases ', '')).date()
  return max_date == datetime.now().date()

is_new = False
file_url = 'https://dshs.texas.gov/coronavirus/TexasCOVID-19NewCasesOverTimebyCounty.xlsx'
count = 0

while is_new is False: 
  print(f'Attempt #{count+1}')
  is_new = check_file()

  if is_new: 
    break
  else:
    time.sleep(300 * (1 + count * 0.05))
    count+=1


print('Running Script')
p = Popen('TMC.bat', cwd=r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID')
stdout, stderr = p.communicate()