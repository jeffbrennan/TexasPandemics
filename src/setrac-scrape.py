import requests
import re
import json
import pandas as pd
from bs4 import BeautifulSoup
from base64 import b64decode
import datetime
import numpy as np


def convert_date(date_num):
    parsed_date = datetime.datetime.fromtimestamp(
        int(date_num / 1000)) + datetime.timedelta(days=1)
    return parsed_date.strftime("%Y-%m-%d")


def parse_data(data_location, level_type):
    dates = []
    bed_values = []
    pt_values = []

    for i, _ in enumerate(data_location):
        raw_date = data_location[i]['C'][0]
        parsed_date = convert_date(raw_date)
        dates.append(parsed_date)

        if len(data_location[i]['C']) == 1:
            pt_values.append(pt_values[i - 1])
            bed_values.append(bed_values[i - 1])

        # data and general beds
        elif len(data_location[i]['C']) == 2:
            pt_values.append(pt_values[i - 1])
            bed_values.append(data_location[i]['C'][1])

        # date, general bed, suspected cases
        elif len(data_location[i]['C']) > 2:
            pt_values.append(data_location[i]['C'][-1])
            bed_values.append(data_location[i]['C'][-2])

        # except IndexError:
        #     bed_values.bed_values(values[i - 1])

    df = pd.DataFrame({'Date': dates,
                       'Level': [f'{level_type}'] * len(dates),
                       'bed_values': bed_values,
                       'pt_values': pt_values})

    complete_dates = pd.period_range('04-01-2020', df['Date'].max())
    complete_dates = complete_dates.strftime('%Y-%m-%d').to_series().to_list()

    # fill missing dates
    df = df.set_index('Date')
    df = df.reindex(complete_dates, fill_value=np.nan)
    df.loc[df['Level'].isna(), 'Level'] = level_type
    df['Date'] = df.index
    df = df.reset_index(drop=True)
    df = df[['Date', 'Level', 'bed_values', 'pt_values']]

    # fix N/As: only real NA date is 5/22 for certain counties
    # other N/As are presented as repeat values of previous day value on the site

    df.loc[(df['Date'] == '2020-05-22') &
           (df['bed_values'].isna()), 'bed_values'] = 'N/A'
    df.fillna(method='ffill', inplace=True)

    return df


def get_tokens(page_url):
    soup = BeautifulSoup(requests.get(page_url).content, 'html.parser')
    page_text = str(soup)

    encoded_resource = re.search('(r=)(.*)(\&)', page_url).group(2)
    resource_key = json.loads(b64decode(encoded_resource))['k']
    request_id = re.search("var requestId = \\'(.*?)\\';", page_text).group(1)
    activity_id = re.search(
        "var telemetrySessionId =  \\'(.*?)\\';", page_text).group(1)

    return resource_key, activity_id, request_id


def get_icu_data(county):
    print(f'Obtaining icu bed usage for {county}')
    payload = {
        'version': '1.0.0',
        'queries': [
            {
                'Query': {
                    'Commands': [
                        {
                            'SemanticQueryDataShapeCommand': {
                                'Query': {
                                    'Version': 2,
                                    'From': [
                                        {
                                            'Name': 'h',
                                            'Entity': 'Hospital_Info',
                                            'Type': 0,
                                        },
                                        {
                                            'Name': 'd',
                                            'Entity': 'Date',
                                            'Type': 0,
                                        },
                                        {
                                            'Name': 's1',
                                            'Entity': 'SETRAC Measures',
                                            'Type': 0,
                                        },
                                        {
                                            'Name': 't',
                                            'Entity': 'TSA_County',
                                            'Type': 0,
                                        },
                                    ],
                                    'Select': [
                                        {
                                            'Aggregation': {
                                                'Expression': {
                                                    'Column': {
                                                        'Expression': {
                                                            'SourceRef': {
                                                                'Source': 'h',
                                                            },
                                                        },
                                                        'Property': 'ICU Bed Surge',
                                                    },
                                                },
                                                'Function': 0,
                                            },
                                            'Name': 'Sum(Hospital_Info.ICU Bed Surge)',
                                        },
                                        {
                                            'Aggregation': {
                                                'Expression': {
                                                    'Column': {
                                                        'Expression': {
                                                            'SourceRef': {
                                                                'Source': 'h',
                                                            },
                                                        },
                                                        'Property': 'Operational ICU Beds',
                                                    },
                                                },
                                                'Function': 0,
                                            },
                                            'Name': 'Sum(Hospital_Info.Operational ICU Beds)',
                                        },
                                        {
                                            'Measure': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 's1',
                                                    },
                                                },
                                                'Property': 'ICU Beds In Use',
                                            },
                                            'Name': 'BedVentData.ICU Beds In Use',
                                        },
                                        {
                                            'Column': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 'd',
                                                    },
                                                },
                                                'Property': 'Date',
                                            },
                                            'Name': 'Date.Date',
                                        },
                                        {
                                            'Measure': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 's1',
                                                    },
                                                },
                                                'Property': 'Patients in Intensive Care Beds (Suspected + Confirmed)',
                                            },
                                            'Name': 'SurvData.Patients in Intensive Care Beds (Suspected + Confirmed)',
                                        },
                                    ],
                                    'Where': [
                                        {
                                            'Condition': {
                                                'Comparison': {
                                                    'ComparisonKind': 2,
                                                    'Left': {
                                                        'Column': {
                                                            'Expression': {
                                                                'SourceRef': {
                                                                    'Source': 'd',
                                                                },
                                                            },
                                                            'Property': 'Date',
                                                        },
                                                    },
                                                    'Right': {
                                                        'Literal': {
                                                            'Value': 'datetime\'2020-04-01T00:00:00\'',
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                        {
                                            'Condition': {
                                                'In': {
                                                    'Expressions': [
                                                        {
                                                            'Column': {
                                                                'Expression': {
                                                                    'SourceRef': {
                                                                        'Source': 't',
                                                                    },
                                                                },
                                                                'Property': 'County',
                                                            },
                                                        },
                                                    ],
                                                    'Values': [
                                                        [
                                                            {
                                                                'Literal': {
                                                                    "Value": f"'{county}'"
                                                                },
                                                            },
                                                        ],
                                                    ],
                                                },
                                            },
                                        },
                                        {
                                            'Condition': {
                                                'Not': {
                                                    'Expression': {
                                                        'Comparison': {
                                                            'ComparisonKind': 0,
                                                            'Left': {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'h',
                                                                        },
                                                                    },
                                                                    'Property': 'Hospital Name',
                                                                },
                                                            },
                                                            'Right': {
                                                                'Literal': {
                                                                    'Value': 'null',
                                                                },
                                                            },
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                                'Binding': {
                                    'Primary': {
                                        'Groupings': [
                                            {
                                                'Projections': [
                                                    0,
                                                    1,
                                                    2,
                                                    3,
                                                    4,
                                                ],
                                            },
                                        ],
                                    },
                                    'DataReduction': {
                                        'DataVolume': 4,
                                        'Primary': {
                                            'BinnedLineSample': {},
                                        },
                                    },
                                    'Version': 1,
                                },
                                'ExecutionMetricsKind': 1,
                            },
                        },
                    ],
                },
                'QueryId': '',
                'ApplicationContext': {
                    'DatasetId': '0d9b903d-f5b8-40ab-8f52-9ac0bed82011',
                    'Sources': [
                        {
                            'ReportId': '4208e1af-ebdd-4c53-bf87-e21ab753bd89',
                            'VisualId': '74a6e32c9c5a3fc8c606',
                        },
                    ],
                },
            },
        ],
        'cancelQueries': [],
        'modelId': 13206353,
    }

    response = requests.post(query_url, json=payload, headers=headers).json()
    data = response['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']
    df = parse_data(data, county)
    return df


def get_gen_data(county):
    print(f'Obtaining general bed usage for {county}')
    payload = {
        'version': '1.0.0',
        'queries': [
            {
                'Query': {
                    'Commands': [
                        {
                            'SemanticQueryDataShapeCommand': {
                                'Query': {
                                    'Version': 2,
                                    'From': [
                                        {
                                            'Name': 'h',
                                            'Entity': 'Hospital_Info',
                                            'Type': 0,
                                        },
                                        {
                                            'Name': 'd',
                                            'Entity': 'Date',
                                            'Type': 0,
                                        },
                                        {
                                            'Name': 's1',
                                            'Entity': 'SETRAC Measures',
                                            'Type': 0,
                                        },
                                        {
                                            'Name': 't',
                                            'Entity': 'TSA_County',
                                            'Type': 0,
                                        },
                                    ],
                                    'Select': [
                                        {
                                            'Aggregation': {
                                                'Expression': {
                                                    'Column': {
                                                        'Expression': {
                                                            'SourceRef': {
                                                                'Source': 'h',
                                                            },
                                                        },
                                                        'Property': 'Operational General Beds',
                                                    },
                                                },
                                                'Function': 0,
                                            },
                                            'Name': 'Sum(Hospital_Info.Operational General Beds)',
                                        },
                                        {
                                            'Aggregation': {
                                                'Expression': {
                                                    'Column': {
                                                        'Expression': {
                                                            'SourceRef': {
                                                                'Source': 'h',
                                                            },
                                                        },
                                                        'Property': 'General Bed Surge',
                                                    },
                                                },
                                                'Function': 0,
                                            },
                                            'Name': 'Sum(Hospital_Info.Operational General Surge)',
                                        },
                                        {
                                            'Measure': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 's1',
                                                    },
                                                },
                                                'Property': 'General Beds in Use',
                                            },
                                            'Name': 'SurvData.Available General Beds',
                                        },
                                        {
                                            'Column': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 'd',
                                                    },
                                                },
                                                'Property': 'Date',
                                            },
                                            'Name': 'Date.Date',
                                        },
                                        {
                                            'Measure': {
                                                'Expression': {
                                                    'SourceRef': {
                                                        'Source': 's1',
                                                    },
                                                },
                                                'Property': 'Patients in General Beds (Suspected + Confirmed)',
                                            },
                                            'Name': 'SurvData.Patients in General Beds (Suspected + Confirmed)',
                                        },
                                    ],
                                    'Where': [
                                        {
                                            'Condition': {
                                                'Comparison': {
                                                    'ComparisonKind': 2,
                                                    'Left': {
                                                        'Column': {
                                                            'Expression': {
                                                                'SourceRef': {
                                                                    'Source': 'd',
                                                                },
                                                            },
                                                            'Property': 'Date',
                                                        },
                                                    },
                                                    'Right': {
                                                        'Literal': {
                                                            'Value': 'datetime\'2020-04-01T00:00:00\'',
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                        {
                                            'Condition': {
                                                'In': {
                                                    'Expressions': [
                                                        {
                                                            'Column': {
                                                                'Expression': {
                                                                    'SourceRef': {
                                                                        'Source': 't',
                                                                    },
                                                                },
                                                                'Property': 'County',
                                                            },
                                                        },
                                                    ],
                                                    'Values': [
                                                        [
                                                            {
                                                                'Literal': {
                                                                    "Value": f"'{county}'"
                                                                },
                                                            },
                                                        ],
                                                    ],
                                                },
                                            },
                                        },
                                        {
                                            'Condition': {
                                                'Not': {
                                                    'Expression': {
                                                        'Comparison': {
                                                            'ComparisonKind': 0,
                                                            'Left': {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'h',
                                                                        },
                                                                    },
                                                                    'Property': 'Hospital Name',
                                                                },
                                                            },
                                                            'Right': {
                                                                'Literal': {
                                                                    'Value': 'null',
                                                                },
                                                            },
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                                'Binding': {
                                    'Primary': {
                                        'Groupings': [
                                            {
                                                'Projections': [
                                                    0,
                                                    1,
                                                    2,
                                                    3,
                                                    4,
                                                ],
                                            },
                                        ],
                                    },
                                    'DataReduction': {
                                        'DataVolume': 4,
                                        'Primary': {
                                            'BinnedLineSample': {},
                                        },
                                    },
                                    'Version': 1,
                                },
                                'ExecutionMetricsKind': 1,
                            },
                        },
                    ],
                },
                'QueryId': '',
                'ApplicationContext': {
                    'DatasetId': '0d9b903d-f5b8-40ab-8f52-9ac0bed82011',
                    'Sources': [
                        {
                            'ReportId': '4208e1af-ebdd-4c53-bf87-e21ab753bd89',
                            'VisualId': 'a7f83cb3f07c93413eae',
                        },
                    ],
                },
            },
        ],
        'cancelQueries': [],
        'modelId': 13206353,
    }

    response = requests.post(query_url, json=payload, headers=headers).json()
    data = response['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']
    df = parse_data(data, county)
    return df


# region setup
page_url = 'https://app.powerbi.com/view?r=eyJrIjoiMWIxODUxMzItYTA1MC00MGEzLTgwNjgtMWYwMzk1OTZjYjI3IiwidCI6ImI3MjgwODdjLTgwZTgtNGQzMS04YjZmLTdlMGUzYmUxMGUwOCIsImMiOjN9&pageName=ReportSection8bf44205545a967578b1'
resource_key, activity_id, request_id = get_tokens(page_url)

headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Accept': 'application/json',
    'X-PowerBI-ResourceKey': resource_key,
    'ActivityId': activity_id,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 Edg/87.0.664.66',
    'RequestId': request_id,
    'Origin': 'https://app.powerbi.com',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://app.powerbi.com/',
    'Accept-Language': 'en-US,en;q=0.9'}
params = (('preferReadOnlySession', 'true'))
query_url = 'https://wabi-us-north-central-api.analysis.windows.net/public/reports/querydata?synchronous=true'
# endregion


# region county setup
icu_df = pd.DataFrame(
    columns=['Date', 'Level', 'bed_values', 'pt_values'])
gen_df = pd.DataFrame(
    columns=['Date', 'Level', 'bed_values', 'pt_values'])

counties = ['Angelina', 'Brazoria', 'Fort Bend', 'Harris',
            'Galveston', 'Jefferson', 'Montgomery', 'Nacogdoches']

for county in counties:
    gen_df = gen_df.append(get_gen_data(county))
    icu_df = icu_df.append(get_icu_data(county))
# endregion

# region prepare merge
gen_df.columns = ['Date', 'Level', 'COVID_General_Beds', 'COVID_General_Patients']
icu_df.columns = ['Date', 'Level', 'COVID_ICU_Beds', 'COVID_ICU_Patients']

combined_df = pd.merge(icu_df, gen_df, on=['Date', 'Level'], how='outer')
date_max = combined_df['Date'].max()

# address instances where NA present in most recent date of df (excludes 5/22)
# done here instead of in parse_data to prevent creation of recent date when data isn't available
combined_df.fillna(method='ffill', inplace=True)
# endregion


# region save
base_directory = 'C:/Users/jeffb/Desktop/Life/personal-projects/COVID/original-sources/historical/setrac/'
combined_df.to_csv(f'{base_directory}setrac_data_{date_max}.csv', index=False)
# endregion
