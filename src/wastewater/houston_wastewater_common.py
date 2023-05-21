import pandas as pd
import json
import requests


def get_offsets(request_url: str, step_interval: int) -> list:
    # retrieves value from rest request "count(*) as n"
    def get_num_records(url) -> int:
        response = requests.get(url)
        output = json.loads(response.content)['features'][0]['attributes']['n']
        return output

    offsets = list(range(0, get_num_records(request_url), step_interval))
    return offsets


def get_data(url_prefix: str, url_suffix: str, offset: int):
    url = f'{url_prefix}{offset}{url_suffix}'
    request = requests.get(url)
    response = json.loads(request.content)['features']
    df = pd.DataFrame.from_records(i['attributes'] for i in response)
    return df


def get_data_manager(url_prefix, url_suffix, offsets: list, max_date: str) -> pd.DataFrame:
    new_df_list = []
    for offset in offsets:
        print(f'Obtaining data with offset: {offset}')

        df = get_data(url_prefix, url_suffix, offset)

        # filtering by date/timestamp in rest query wasn't working
        df_new = (
            df
            .query('date > @max_date')
        )
        if df_new.empty:
            break

        new_df_list.append(df_new)

    new_df_combined = pd.concat(new_df_list)
    return new_df_combined
