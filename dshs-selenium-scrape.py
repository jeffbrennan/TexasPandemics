import pandas as pd
import os 
import time 
import glob
import sys
import bisect

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def build_selenium():
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


def fix_filenames(zero_indices, missing_indices): 
    # remove corrupted files & reset names to avoid overwite conflict
    zero_counties = [counties[i] for i in zero_indices]
    [os.remove(f'{case_directory}\{i}.csv') for i in zero_counties]

    # update file names to correct offsets
    good_counties = [county for x,county in enumerate(counties) if x not in missing_indices]
    for i, original_name in enumerate(glob.glob(f'{case_directory}\*')):
        os.rename(original_name, f'{case_directory}\{good_counties[i]}.csv')


def calc_missing(terminal_indices, zero_indices):
    # actual missing data (located at terminal ends of each driver iteration)    
    missing_indices = []
    offsets = [bisect.bisect(terminal_indices, i) for i in zero_indices]
    for i in offsets: 
        missing_indices.append(terminal_indices[i])
        terminal_indices[i] -= 1
    
    return missing_indices 


def fix_missing(driver):
    # files sometimes are corrupted and have size = 0 bytes
    # i-2: get terminal county position (offset of 1 from enumerate)
    # TODO: try to simplify by starting at 0
    terminal_indices = [i-2 for i in range(1,len(counties)+1) if i%n_tabs == 0] + [len(counties)]  
    file_sizes = [os.path.getsize(i) for i in glob.glob(f'{case_directory}\*')]
    
    # corrupted file
    zero_indices = [i for i,size in enumerate(file_sizes) if size == 0]

    # redo 0 byte county files until none remain
    while len(zero_indices) > 0: 
        missing_indices = calc_missing(terminal_indices, zero_indices)
        missing_counties = [counties[i] for i in missing_indices]
        fix_filenames(zero_indices, missing_indices)

        # rerun missing counties with elongated delay
        for i, county in enumerate(missing_counties): 
            run_scraper(i, county, driver, 1)

        # recheck files
        file_sizes = [os.path.getsize(i) for i in glob.glob(f'{case_directory}\*')]
        zero_indices = [i for i,size in enumerate(file_sizes) if size == 0]

        
def scrape_data(i, county, driver, start_files, sleep_time): 
    site_url = f'https://tabexternal.dshs.texas.gov/t/THD/views/COVIDExternalQC/COVIDTrends?County={county}&:isGuestRedirectFromVizportal=y&:embed=y'
    
    # if first run or multiple of 50, open as new window, otherwise open new tab (3x performance boost)
    if (i == 1) or (i%n_tabs == 0):
        driver.get(site_url)
    else:   
        driver.execute_script(f'''window.open("{site_url}","_blank");''')
        driver.switch_to.window(driver.window_handles[-1])

    # use CSS selectors for small performance gain [obtained via https://addons.mozilla.org/en-US/firefox/addon/selenium-ide/]
    wait = WebDriverWait(driver, 3)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".tab-icon-download"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fppw03o:nth-child(4)"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".thumbnail-wrapper_f1b6thlj:nth-child(3) .hidden-icon_fehyjhr"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fdiufnn"))).click()

    # only exit once file has been added
    # shorter sleep time more likely to corrupt file. Overall faster to accept some corruption and retroactively fix
    while True:
        if len([i for i in os.listdir(case_directory) if i.endswith('.csv')]) == start_files:
            time.sleep(sleep_time)
        else:
            break


def run_scraper(i, county, driver, sleep_time=0.2): 
        # Get current number of .csv files in target directory
        start_files = len([i for i in os.listdir(case_directory) if i.endswith('.csv')])

        # change default filename to county name
        scrape_data(i, county, driver, start_files, sleep_time)
        os.rename(f'{case_directory}\Texas County Cases per Day (2)_crosstab.csv',
                  f'{case_directory}\{county}.csv')
        
        # measure total elapsed time
        run_time = int(time.time() - start_time)
        print(f'[{run_time:04d} sec | #{i:03}]: {county}')

def main(): 
    # loop through counties, restarting driver every n_tabs for stability
    for i, county in enumerate(counties, 1):
        if i == 1: 
            driver = build_selenium()
        elif i%n_tabs == 0: 
            driver.quit()
            driver = build_selenium()
        
        run_scraper(i, county, driver)

    # identify and replace any missing counties
    # driver = build_selenium()
    fix_missing(driver)
    driver.quit()


# SETUP 
start_time = time.time()
base_directory = r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\original-sources'
case_directory = f'{base_directory}\dshs-new-cases'

# wipe directory from previous day (> 30000 sec old) or TMC run (< 10)
county_files = glob.glob(f'{case_directory}\*')
[os.remove(f) for f in county_files if ((start_time - os.path.getctime(f)) > 30000 or len(county_files) < 10)]

if sys.argv[1] == 'all': 
    counties = pd.read_excel(f'{base_directory}\county_classifications.xlsx',
                                sheet_name=0)['County Name'].tolist()
else: 
    counties = sys.argv[1].split(',')

# len(counties) + 1 so new window is never created
n_tabs = len(counties)+1 if len(counties) < 50 else 50

if __name__ == '__main__':
    main()