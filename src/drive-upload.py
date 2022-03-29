from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import pandas as pd
import pickle
import os
import glob

os.chdir(r'C:\Users\jeffb\Desktop\Life\personal-projects\COVID')
CLIENT_SECRET_FILE = 'backend/api/oauth_credentials.json'
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']
FOLDER_IDS = pd.read_csv('backend/api/folder_ids.csv').squeeze()
MIMETYPES = {'.csv': 'text/csv', '.xlsx': 'application/vnd.ms-excel'}


# https://medium.com/analytics-vidhya/accessing-google-sheets-using-python-2ceb3b5cd3ab
def Create_Service(client_secret_file, api_name, api_version, *scopes):
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]

    cred = None
    pickle_file = f'backend/api/token_{API_SERVICE_NAME}_{API_VERSION}.pickle'

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print('Unable to connect.')
        print(e)
        return None


def Get_Drive_IDS(folder_Name):
    query = f"parents = '{FOLDER_IDS[folder_Name]}'"
    response = service.files().list(q=query).execute()
    files = response.get('files')
    return files


def Update_File(file_path, file_data):
    parent_folder = os.path.dirname(file_path)
    media_content = MediaFileUpload(file_path, mimetype=file_data['mimeType'])

    file = service.files().update(
        fileId=file_data['id'],
        media_body=media_content
    ).execute()
    print(f'Updated {parent_folder}\\{file["name"]}')


# connect to drive api & get list of all file ids
service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)
id_results = [Get_Drive_IDS(f) for f in ['tableau', 'tableau\\sandbox']]
all_files = [item for sublist in id_results for item in sublist]

# get relative local file paths
local_files = [i for i in glob.glob('tableau/**', recursive=True) if ('.csv' in i or '.xlsx' in i)]

for f in all_files:
    if f['mimeType'] != 'application/vnd.google-apps.folder':
        f_path = [i for i in local_files if f['name'] in i][0]
        Update_File(f_path, f)
