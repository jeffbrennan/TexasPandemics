import pandas as pd
import os 
import time 
import glob
import sys

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def build_selenium(case_directory):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")     # run in background
    chrome_options.add_argument("--disable-gpu")  # laptop has no dedicated gpu
    chrome_options.add_argument('--log-level=3')  # suppress all console logs

    # enable headless downloading
    chrome_options.add_experimental_option("prefs", {
            "download.default_directory": case_directory,
            "download.prompt_for_download": False,
            "safebrowsing_for_trusted_sources_enabled": False,
            "safebrowsing.enabled": False   
    })

    driver = webdriver.Chrome(executable_path=r"C:\Program Files\chromedriver.exe", options=chrome_options)
    return driver


def fix_missing(case_directory, driver, start_time): 
    # files sometimes are corrupted and have size = 0 bytes

    # get indices of all files with size = 0
    file_sizes = [os.path.getsize(i) for i in glob.glob(f'{case_directory}\*')]
    zero_index = [i for i,size in enumerate(file_sizes) if size == 0]

    # loop through 0 byte county files until none remain
    while len(zero_index) > 0: 

        zero_counties = [os.listdir(case_directory)[i] for i in zero_index]

        county_names = [zero_counties.replace('.csv', '') for county in zero_counties]
        [os.remove(i) for i in f'{case_directory}\{zero_counties[i]}']

        for i, county in enumerate(county_names): 
            run_scraper(i, county, driver, start_time, case_directory)

        # recheck files
        file_sizes = [os.path.getsize(i) for i in glob.glob(f'{case_directory}\*')]
        zero_index = [i for i,size in enumerate(file_sizes) if size == 0]

        
def scrape_data(i, county, driver, start_files, case_directory): 
    site_url = f'https://tabexternal.dshs.texas.gov/t/THD/views/COVIDExternalQC/COVIDTrends?County={county}&:isGuestRedirectFromVizportal=y&:embed=y'
    
    # if first run or multiple of 25, open as new window, otherwise open new tab (3x performance boost)
    if (i == 1) or (i%30 == 0):
        driver.get(site_url)
    else:   
        driver.execute_script(f'''window.open("{site_url}","_blank");''')
        driver.switch_to.window(driver.window_handles[-1])
        # time.sleep(0.5)

    # use CSS selectors for small performance gain [obtained via https://addons.mozilla.org/en-US/firefox/addon/selenium-ide/]
    wait = WebDriverWait(driver, 3)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".tab-icon-download"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fppw03o:nth-child(4)"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".thumbnail-wrapper_f1b6thlj:nth-child(3) .hidden-icon_fehyjhr"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fdiufnn"))).click()

    # only exit once file has been added
    while True:
        if len([i for i in os.listdir(case_directory) if i.endswith('.csv')]) == start_files:
            time.sleep(0.8)
        else:
            break


def run_scraper(i, county, driver, start_time, case_directory): 
        # Get current number of .csv files in target directory
        start_files = len([i for i in os.listdir(case_directory) if i.endswith('.csv')])

        # change default filename to county name
        scrape_data(i, county, driver, start_files, case_directory)
        os.rename(f'{case_directory}\Texas County Cases per Day (2)_crosstab.csv',
                  f'{case_directory}\{county}.csv')
        
        # measure total elapsed time
        run_time = int(time.time() - start_time)
        print(f'[{run_time:04d} sec | #{i:03}]: {county}')


def setup():
    start_time = time.time()
    base_directory = r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\original-sources'
    case_directory = f'{base_directory}\dshs-new-cases'
    
    # wipe directory from previous day (> 10000 sec old) or TMC run (< 10)
    counties = glob.glob(f'{case_directory}\*')
    [os.remove(f) for f in counties if ((start_time - os.path.getctime(f)) > 10000 or len(counties) < 10)]
    
    if sys.argv[1] == 'all': 
        counties = pd.read_excel(f'{base_directory}\county_classifications.xlsx',
                                    sheet_name=0)['County Name'].tolist()
    else: 
        counties = sys.argv[1].split(',')

    return start_time, counties, case_directory


def main(): 
    start_time, counties, case_directory = setup()

    # loop through counties, restarting driver every 25 for stability
    for i, county in enumerate(counties, 1):
        if i == 1: 
            driver = build_selenium(case_directory)
        elif i%30 == 0: 
            driver.quit()
            driver = build_selenium(case_directory)
        
        run_scraper(i, county, driver, start_time, case_directory)

    # identify and replace any missing counties
    fix_missing(case_directory, driver, start_time)
    driver.quit()


if __name__ == '__main__':
    main()
