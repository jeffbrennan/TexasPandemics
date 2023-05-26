import pandas as pd
from tableauscraper import TableauScraper as TS
from tableauscraper import TableauWorkbook
from src.utils import write_file
import yaml
from pathlib import Path
from src.county_vitals.request_common import clean_request_data


def explore_data(workbook: TableauWorkbook) -> None:
    # pick a sheet
    sheets = workbook.getSheets()
    for sheet in sheets:
        print(sheet)

    target_sheet_name = 'County Trends'
    # go to sheet
    target_sheet = workbook.goToSheet(target_sheet_name)

    # explore worksheets
    for sheet in target_sheet.getWorksheets():
        print(sheet.name)
        print(sheet.data)

    target_worksheet_name = 'Positives w/ MA'

    result = target_sheet.getWorksheet(target_worksheet_name).data
    return result

    for sheet in workbook.getWorksheets():
        print(sheet.name)
        print(sheet.data)


def get_data(workbook: TableauWorkbook, config: dict) -> pd.DataFrame | None:
    target_sheet_name = config['target_sheet']
    target_worksheet_name = config['target_worksheet']

    try:
        target_sheet = workbook.goToSheet(target_sheet_name)
        result = target_sheet.getWorksheet(target_worksheet_name).data
    except Exception as e:
        print(f'Error: {e}')
        result = None
    return result


def create_workbook(url: str) -> TableauWorkbook:
    ts = TS()
    ts.loads(url)
    workbook = ts.getWorkbook()
    return workbook


def get_vitals(config: dict) -> str:
    workbook = create_workbook(config['url'])
    raw_data = get_data(workbook, config)

    if raw_data is None:
        return 'Failed to get data'

    clean_df = clean_request_data(raw_data, config)
    write_file(clean_df, f'{config["out"]["dir"]}/{config["out"]["table_name"]}')
    return 'Success'


CONFIG = yaml.safe_load(Path('src/county_vitals/arcgis_rest/tableau_config.yaml').read_text())
counties = list(CONFIG.keys())
counties = ['galveston']
[get_vitals(CONFIG[county]) for county in counties]
