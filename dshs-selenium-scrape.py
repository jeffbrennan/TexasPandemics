import pandas as pd
import os 
import time 
import glob

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# TODO: continue trying to scrape POST/GET requests instead of automating clicking on download 
# was experiencing [400] and [500 errors with previous attempts (dshs-scrape.py)]

def build_selenium(case_directory):
    options = Options()

    # TODO: // bottleneck - download doesn't work with headless but can be supposedly modified
    # options.add_argument('--headless')  # suppresses the browser from opening
    # options.add_argument('--disable-gpu')
    options.add_argument("--log-level=3")  # suppresses console errors

    prefs = {'download.default_directory' : case_directory}
    options.add_experimental_option('prefs', prefs)

    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
    return driver


def scrape_data(driver, county, start_files, case_directory): 
    
    driver.get(f'https://tabexternal.dshs.texas.gov/t/THD/views/COVIDExternalQC/COVIDTrends?County={county}&:isGuestRedirectFromVizportal=y&:embed=y')

    wait = WebDriverWait(driver, 4)
    wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(.,'Download')]"))).click()
    wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(.,'Crosstab')]"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".thumbnail-wrapper_f1b6thlj:nth-child(3) .hidden-icon_fehyjhr"))).click()
    driver.find_element(By.XPATH, '//*[@id="export-crosstab-options-dialog-Dialog-BodyWrapper-Dialog-Body-Id"]/div/div[2]/button').click()

    # # TODO: //bottleneck (ideally would sleep only until file immediately appears)
    # n*254*2 list comprehensions - investigate performance impact
    while True:
        if len([i for i in os.listdir(case_directory) if i.endswith('.csv')]) == start_files:
            time.sleep(0.5)
        else:
            driver.close()
            driver.quit()
            break


# SETUP
case_directory = r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\original-sources\dshs-new-cases'

counties = pd.read_excel(r"C:\Users\jeffb\Desktop\Life\personal-projects\COVID\original-sources\county_classifications.xlsx",
                            sheet_name=0)['County Name'].tolist()

# WIPE DIRECTORY FROM PREVIOUS DAY 
files = glob.glob(case_directory)
for f in files:
    os.remove(f)

# SCRAPE NEW DATA
for i, county in enumerate(counties, 1): 
    print(f'({i:03}/{len(counties)}): {county}')
    start_time = time.time()

    # Get current number of files in target directory
    n_files = len([i for i in os.listdir(case_directory) if i.endswith('.csv')])

    # TODO: build driver each time (avoids "selenium max retries exceeded with url") // bottleneck
    driver = build_selenium(case_directory)
    scrape_data(driver, county, n_files, case_directory)
    run_time = time.time() - start_time
    print(f'{run_time:.2f} seconds')

driver.quit()