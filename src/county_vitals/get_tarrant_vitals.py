# post request
# https://stackoverflow.com/questions/71373590/web-scraping-with-python-requests-post-request
# https://www.tarrantcountytx.gov/en/public-health/disease-control---prevention/COVID-19.html
import pandas as pd
import requests
import json
from src.utils import convert_timestamp, write_file

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'ActivityId': '7344a56b-f125-426a-b60b-146cd92364fa',
    'RequestId': 'b86f4a37-b68c-2ef8-08b0-bd803d95407c',
    'X-PowerBI-ResourceKey': 'b830a566-31aa-44aa-a9d0-bec2a4f88118',
    'Content-Type': 'application/json;charset=UTF-8',
    'Origin': 'https://app.powerbigov.us',
    'Connection': 'keep-alive',
    'Referer': 'https://app.powerbigov.us/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
}
json_data = {
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
                                        'Name': 'c1',
                                        'Entity': 'CV19_Case Data by Date',
                                        'Type': 0,
                                    },
                                    {
                                        'Name': 'c',
                                        'Entity': 'CV19_CLI',
                                        'Type': 0,
                                    },
                                ],
                                'Select': [
                                    {
                                        'Column': {
                                            'Expression': {
                                                'SourceRef': {
                                                    'Source': 'c1',
                                                },
                                            },
                                            'Property': 'Week',
                                        },
                                        'Name': 'CV19_Case Data by Date.Week ',
                                    },
                                    {
                                        'Aggregation': {
                                            'Expression': {
                                                'Column': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': 'c1',
                                                        },
                                                    },
                                                    'Property': 'Probable_Specimen Date',
                                                },
                                            },
                                            'Function': 0,
                                        },
                                        'Name': 'Sum(CV19_Case Data by Date.Probable_Specimen Date)',
                                    },
                                    {
                                        'Aggregation': {
                                            'Expression': {
                                                'Column': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': 'c1',
                                                        },
                                                    },
                                                    'Property': 'Confirm_Specimen Date',
                                                },
                                            },
                                            'Function': 0,
                                        },
                                        'Name': 'Sum(CV19_Case Data by Date.Confirm_Specimen Date)',
                                    },
                                    {
                                        'Aggregation': {
                                            'Expression': {
                                                'Column': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': 'c1',
                                                        },
                                                    },
                                                    'Property': '7Day_Spec',
                                                },
                                            },
                                            'Function': 0,
                                        },
                                        'Name': 'Sum(CV19_Case Data by Date.7Day_Spec)',
                                    },
                                    {
                                        'Aggregation': {
                                            'Expression': {
                                                'Column': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': 'c1',
                                                        },
                                                    },
                                                    'Property': 'Cases_Specimen Date',
                                                },
                                            },
                                            'Function': 0,
                                        },
                                        'Name': 'Sum(CV19_Case Data by Date.Cases_Specimen Date)',
                                    },
                                ],
                                'Where': [
                                    {
                                        'Condition': {
                                            'In': {
                                                'Expressions': [
                                                    {
                                                        'Column': {
                                                            'Expression': {
                                                                'SourceRef': {
                                                                    'Source': 'c',
                                                                },
                                                            },
                                                            'Property': 'Year',
                                                        },
                                                    },
                                                ],
                                                'Values': [
                                                    [
                                                        {
                                                            'Literal': {
                                                                'Value': "'2020'",
                                                            },
                                                        },
                                                    ],
                                                    [
                                                        {
                                                            'Literal': {
                                                                'Value': "'2022-2023'",
                                                            },
                                                        },
                                                    ],
                                                    [
                                                        {
                                                            'Literal': {
                                                                'Value': "'2021'",
                                                            },
                                                        },
                                                    ],
                                                ],
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
                                                3,
                                                2,
                                                1,
                                                4,
                                            ],
                                        },
                                    ],
                                },
                                'DataReduction': {
                                    'DataVolume': 4,
                                    'Primary': {
                                        'Sample': {},
                                    },
                                },
                                'SuppressedJoinPredicates': [
                                    4,
                                ],
                                'Version': 1,
                            },
                        },
                    },
                ],
            },
            'QueryId': '',
        },
    ],
    'cancelQueries': [],
    'modelId': 383287,
}

response = requests.post(
    'https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net/public/reports/querydata',
    headers=headers,
    json=json_data,
)


def get_data(headers: dict, payload: dict) -> dict:
    response = requests.post(
        'https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net/public/reports/querydata',
        headers=headers,
        json=payload,
    )

    response_json = json.loads(response.content)
    return response_json


def parse_response(response_json: dict) -> pd.DataFrame:
    result_data_source = response_json['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']
    result_data_series = pd.DataFrame(result_data_source)['C']
    result_df = pd.DataFrame(result_data_series.tolist())
    return result_df


def clean_data(result_df: pd.DataFrame) -> pd.DataFrame:
    clean_df = (
        result_df
        .rename(columns={0: 'Date', 1: 'probable', 2: 'confirmed', 3: '7day', 4: 'cases_daily'})
        .assign(Date=lambda x: convert_timestamp(x['Date']))
        .astype({'cases_daily': 'int32[pyarrow]'})
        .assign(County='Tarrant')
        [['Date', 'cases_daily']]
    )
    return clean_df


def get_tarrant_vitals() -> None:
    response = get_data(headers, json_data)
    result_df = parse_response(response)
    clean_df = clean_data(result_df)
    write_file(clean_df, 'tableau/vitals/staging/tarrant_vitals')


get_tarrant_vitals()
