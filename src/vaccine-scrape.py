import pandas as pd
import os
import time
import glob
import sys
import bisect
import concurrent.futures
from itertools import repeat

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from joblib import Parallel, delayed


def build_selenium():
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("--headless")     # run in background
    chrome_options.add_argument('--log-level=3')  # suppress all console logs

    # enable headless downloading
    chrome_options.add_experimental_option("prefs", {
            "download.default_directory": r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\orignal-sources\historical\vax-dashboard\',
            "download.prompt_for_download": False,
            "safebrowsing_for_trusted_sources_enabled": False,
            "safebrowsing.enabled": False
    })

    driver = webdriver.Chrome(executable_path=r"C:\Program Files\chromedriver.exe", options=chrome_options)
    return driver


def scrape_alloc(county, i, driver, sleep_time, start_files):
    site_url = f"https://tabexternal.dshs.texas.gov/t/THD/views/COVID-19VaccineinTexasDashboard/VaccineDosesAllocated?County={county}&:isGuestRedirectFromVizportal=y&:embed=y"

    if (i == 1) or (i % n_tabs == 0):
        driver.get(site_url)
    else:
        driver.execute_script(f'''window.open("{site_url}","_blank");''')
        driver.switch_to.window(driver.window_handles[-1])

    wait = WebDriverWait(driver, 1)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#download-ToolbarButton"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fppw03o:nth-child(4)"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".thumbnail-wrapper_f1b6thlj:nth-child(3) .hidden-icon-wrapper_f72siau"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fdiufnn"))).click()

    while True:
        if len([i for i in os.listdir(vax_directory) if i.endswith('.csv')]) == start_files:
            time.sleep(sleep_time)
        else:
            break

    while True:
        if any(['By Allocated Wk_crosstab' in i for i in os.listdir(vax_directory)]):
            time.sleep(sleep_time)
            os.rename(f'{vax_directory}/By Allocated Wk_crosstab.csv',
                      f'{vax_directory}/alloc/allocation_week_{county}.csv')
        else:
            break


def scrape_admin(county, i, driver, sleep_time, start_files):
    vax_directory = 'C:/Users/jeffb/Desktop/Life/personal-projects/COVID-testing/vax_scrape/'
    site_url = f"https://tabexternal.dshs.texas.gov/t/THD/views/COVID-19VaccineinTexasDashboard/VaccineDosesAdministered?County={county}&%3AisGuestRedirectFromVizportal=y&%3Aembed=y"

    if (i == 1) or (i % n_tabs == 0):
        driver.get(site_url)
    else:
        driver.execute_script(f'''window.open("{site_url}","_blank");''')
        driver.switch_to.window(driver.window_handles[-1])

    wait = WebDriverWait(driver, 1)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#download-ToolbarButton"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fppw03o:nth-child(4)"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".thumbnail-wrapper_f1b6thlj:nth-child(3) .hidden-icon-wrapper_f72siau"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fdiufnn"))).click()

    while True:
        if len([i for i in os.listdir(vax_directory) if i.endswith('.csv')]) == start_files:
            time.sleep(sleep_time)
        else:
            break

    while True:
        if any(['Doses by Week_crosstab' in i for i in os.listdir(vax_directory)]):
            time.sleep(sleep_time)
            os.rename(f'{vax_directory}/Doses by Week_crosstab.csv',
                      f'{vax_directory}/admin/admin_week_{county}.csv')
        else:
            break


def scrape_demo(county, i, driver, sleep_time, start_files):
    site_url = f"https://tabexternal.dshs.texas.gov/t/THD/views/COVID-19VaccineinTexasDashboard/PeopleVaccinated?County={county}&%3AisGuestRedirectFromVizportal=y&%3Aembed=y"

    if (i == 1) or (i % n_tabs == 0):
        driver.get(site_url)
    else:
        driver.execute_script(f'''window.open("{site_url}","_blank");''')
        driver.switch_to.window(driver.window_handles[-1])

    time.sleep(3)
    driver.find_element_by_css_selector("#\[Parameters\]\.\[Parameter\ 1\]_1 > div.facetOverflow > input").click()
    wait = WebDriverWait(driver, 5)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#download-ToolbarButton"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fppw03o:nth-child(4)"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".thumbnail-wrapper_f1b6thlj:nth-child(2) .hidden-icon_fehyjhr"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fdiufnn"))).click()

    # while True:
    #     try:
    #         wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#download-ToolbarButton"))).click()
    #         wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fppw03o:nth-child(4)"))).click()
    #         wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".thumbnail-wrapper_f1b6thlj:nth-child(2) .hidden-icon_fehyjhr"))).click()
    #         wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fdiufnn"))).click()
    #     except TimeoutError:
    #         wait = WebDriverWait(driver, 5)
    #         continue
    #     break

    while True:
        if len([i for i in os.listdir(vax_directory) if i.endswith('.csv')]) == start_files:
            time.sleep(sleep_time)
        else:
            break

    while True:
        if any(['People by Age_crosstab' in i for i in os.listdir(vax_directory)]):
            time.sleep(sleep_time)
            os.rename(f'{vax_directory}/People by Age_crosstab.csv',
                      f'{vax_directory}/demo_age/age_{county}.csv')
        else:
            break

    time.sleep(1)

    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#download-ToolbarButton"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fppw03o:nth-child(4)"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".thumbnail-wrapper_f1b6thlj:nth-child(3) .hidden-icon_fehyjhr"))).click()
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".fdiufnn"))).click()

    while True:
        if len([i for i in os.listdir(vax_directory) if i.endswith('.csv')]) == start_files:
            time.sleep(sleep_time)
        else:
            break

    time.sleep(1)

    os.rename(f'{vax_directory}/People by R_E_crosstab.csv',
              f'{vax_directory}/demo_race/race_{county}.csv')


def run_scraper(county, i, sleep_time=1):
    start_files = len([i for i in os.listdir(vax_directory) if i.endswith('.csv')])

    try:
        scrape_alloc(county, i, driver, sleep_time, start_files)
        scrape_admin(county, i, driver, sleep_time, start_files)
        scrape_demo(county, i, driver, sleep_time, start_files)
        print(f'Finished: {county}')

    except TimeoutError:
        print(f'Failed: {county}')
        pass


if __name__ == '__main__':
    n_tabs = 60

    vax_directory = 'C:/Users/jeffb/Desktop/Life/personal-projects/COVID/original-sources/historical/vax-dashboard'
    counties = pd.read_csv('C:/Users/jeffb/Desktop/Life/personal-projects/COVID/tableau/county_vaccine.csv')['County']
    county_list = list(set(counties.to_list()))
    county_list.sort()

    county_files = glob.glob(f'{vax_directory}\*')
    start_time = time.time()
    expiration_time = 6 * 24 * 60 * 60

    [os.remove(f) for f in county_files if ((start_time - os.path.getctime(f)) > expiration_time]

    for i, county in enumerate(county_list, 1):
        if i == 1:
            driver = build_selenium()
        elif i % n_tabs == 0:
            driver.quit()
            driver = build_selenium()
        run_scraper(county, i)
