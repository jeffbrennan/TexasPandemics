import pandas as pd
import os
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

def build_selenium():
    options = Options()
    options.add_argument('--headless')  # suppresses the browser from opening
    options.add_argument('--disable-gpu')
    options.add_argument("--log-level=3")  # suppresses console errors

    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
    return driver


# get texas schools from the NYTG_schools json object
def get_data(driver):
    driver.get('https://www.nytimes.com/interactive/2020/us/covid-college-cases-tracker.html')
    texas_schools = driver.execute_script("""const texas_schools = (NYTG_schools.filter((s) => 
                                                                    s['state'] === 'Texas'));
                                             return texas_schools""")

    nyt_date = driver.find_element_by_xpath('/html/body/div[1]/main/article/header/div[2]/p/time').text

    driver.quit()
    return [texas_schools, nyt_date]


# parse json into df and output to csv
def parse_data(nyt_data):
    texas_schools = nyt_data[0]
    nyt_date = nyt_data[1]

    date_formatted = nyt_date.replace('Updated ', '').replace('Sept', 'Sep')  # temp fix to coerce September into standard format
    date_out = datetime.strptime(date_formatted, '%b. %d, %Y')

    df = pd.DataFrame(texas_schools)[['nytname', 'city', 'county', 'death', 'infected']]
    df['Date'] = date_out
    
    df.to_csv('original-sources/historical/nyt-colleges/nyt_colleges_' + date_out.strftime('%Y-%m-%d') + '.csv',
              index=False)


# set wd
os.chdir(r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID')

driver = build_selenium()
parse_data(get_data(driver))
