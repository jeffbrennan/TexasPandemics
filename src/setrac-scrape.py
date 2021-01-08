import requests
import re
import json
import pandas as pd
from bs4 import BeautifulSoup
from base64 import b64decode
import datetime


def convert_date(date_num): 
    parsed_date = datetime.datetime.fromtimestamp(int(date_num / 1000)) + datetime.timedelta(days=1)
    return parsed_date.strftime("%Y-%m-%d")


def parse_data(data_location, level_type):
    dates = []
    covid_icu = []

    for i, _ in enumerate(data_location):
        raw_date = data_location[i]['C'][0]
        parsed_date = convert_date(raw_date)
        dates.append(parsed_date)

        covid_icu.append(data_location[i]['C'][-1])

    df = pd.DataFrame({'Date': dates,
                       'Level': [f'{level_type}'] * len(dates),
                       'COVID_ICU': covid_icu})

    return df


def get_tokens(page_url):
    soup = BeautifulSoup(requests.get(page_url).content, 'html.parser')
    page_text = str(soup)

    encoded_resource = re.search('(r=)(.*)(\&)', page_url).group(2)
    resource_key = json.loads(b64decode(encoded_resource))['k']
    request_id = re.search("var requestId = \\'(.*?)\\';", page_text).group(1)
    activity_id = re.search("var telemetrySessionId =  \\'(.*?)\\';", page_text).group(1)

    return resource_key, activity_id, request_id


page_url = 'https://app.powerbi.com/view?r=eyJrIjoiOWU1ZTQ0MmQtYjEzNi00N2FlLTg0OTYtMjkzYjU4OWEwMzY5IiwidCI6ImI3MjgwODdjLTgwZTgtNGQzMS04YjZmLTdlMGUzYmUxMGUwOCIsImMiOjN9&pageName=ReportSectionc4498c097c625609dc1d'

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

tsa_payload = {"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"h","Entity":"Hospital_Info","Type":0},{"Name":"d","Entity":"Date","Type":0},{"Name":"s1","Entity":"SETRAC Measures","Type":0},{"Name":"t","Entity":"TSA_County","Type":0}],"Select":[{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"h"}},"Property":"ICU Bed Surge"}},"Function":0},"Name":"Sum(Hospital_Info.ICU Bed Surge)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"h"}},"Property":"Operational ICU Beds"}},"Function":0},"Name":"Sum(Hospital_Info.Operational ICU Beds)"},{"Measure":{"Expression":{"SourceRef":{"Source":"s1"}},"Property":"ICU Beds In Use"},"Name":"BedVentData.ICU Beds In Use"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Date"},"Name":"Date.Date"},{"Measure":{"Expression":{"SourceRef":{"Source":"s1"}},"Property":"Patients in Intensive Care Beds (Suspected + Confirmed)"},"Name":"SurvData.Patients in Intensive Care Beds (Suspected + Confirmed)"}],"Where":[{"Condition":{"Comparison":{"ComparisonKind":2,"Left":{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Date"}},"Right":{"Literal":{"Value":"datetime'2020-12-13T00:00:00'"}}}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"t"}},"Property":"TSA"}}],"Values":[[{"Literal":{"Value":"'TSA_Q'"}}]]}}},{"Condition":{"Not":{"Expression":{"Comparison":{"ComparisonKind":0,"Left":{"Column":{"Expression":{"SourceRef":{"Source":"h"}},"Property":"Hospital Name"}},"Right":{"Literal":{"Value":"null"}}}}}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4]}]},"DataReduction":{"DataVolume":4,"Primary":{"BinnedLineSample":{}}},"Version":1}}}]},"QueryId":"","ApplicationContext":{"DatasetId":"92bf5cbe-7a49-4548-a0b0-82e6a1d9ed1d","Sources":[{"ReportId":"db2a8e7a-2dc2-4ccd-8f8d-7aa98f5d424b"}]}}],"cancelQueries":[],"modelId":12328350}

harris_payload = {"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"h","Entity":"Hospital_Info","Type":0},{"Name":"d","Entity":"Date","Type":0},{"Name":"s1","Entity":"SETRAC Measures","Type":0},{"Name":"t","Entity":"TSA_County","Type":0}],"Select":[{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"h"}},"Property":"ICU Bed Surge"}},"Function":0},"Name":"Sum(Hospital_Info.ICU Bed Surge)"},{"Aggregation":{"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"h"}},"Property":"Operational ICU Beds"}},"Function":0},"Name":"Sum(Hospital_Info.Operational ICU Beds)"},{"Measure":{"Expression":{"SourceRef":{"Source":"s1"}},"Property":"ICU Beds In Use"},"Name":"BedVentData.ICU Beds In Use"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Date"},"Name":"Date.Date"},{"Measure":{"Expression":{"SourceRef":{"Source":"s1"}},"Property":"Patients in Intensive Care Beds (Suspected + Confirmed)"},"Name":"SurvData.Patients in Intensive Care Beds (Suspected + Confirmed)"}],"Where":[{"Condition":{"Comparison":{"ComparisonKind":2,"Left":{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"Date"}},"Right":{"Literal":{"Value":"datetime'2020-12-13T00:00:00'"}}}}},{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"t"}},"Property":"County"}}],"Values":[[{"Literal":{"Value":"'Harris'"}}]]}}},{"Condition":{"Not":{"Expression":{"Comparison":{"ComparisonKind":0,"Left":{"Column":{"Expression":{"SourceRef":{"Source":"h"}},"Property":"Hospital Name"}},"Right":{"Literal":{"Value":"null"}}}}}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4]}]},"DataReduction":{"DataVolume":4,"Primary":{"BinnedLineSample":{}}},"Version":1}}}]},"QueryId":"","ApplicationContext":{"DatasetId":"92bf5cbe-7a49-4548-a0b0-82e6a1d9ed1d","Sources":[{"ReportId":"db2a8e7a-2dc2-4ccd-8f8d-7aa98f5d424b"}]}}],"cancelQueries":[],"modelId":12328350}

tsa_response = requests.post(query_url, json=tsa_payload, headers=headers).json()
tsa_data = tsa_response['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']

harris_response = requests.post(query_url, json=harris_payload, headers=headers).json()
harris_data = harris_response['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']


tsa_df = parse_data(tsa_data, 'TSA Q')
harris_df = parse_data(harris_data, 'Harris')

combined_df = tsa_df.append(harris_df)
date_max = combined_df['Date'].max()

base_directory = r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID\original-sources\historical\setrac'
combined_df.to_csv(f'{base_directory}\\setrac_data_{date_max}.csv', index=False)
