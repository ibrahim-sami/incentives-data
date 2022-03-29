import os
import logging
import json
import time
import random

from google.cloud import bigquery
from google.cloud import logging as cloudlogging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import googleapiclient.errors
import pygsheets


COLUMNS = [
    'year', 'month', 'week', 
    'employee_id', 'agent_name',
    'agent_assignment', 'total_amount',
    'TL_performance','project', 'delivery_center'
]


def setup_logging(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    lg_client = cloudlogging.Client(project='hub-data-295911')
    lg_handler = lg_client.get_default_handler()
    # lg_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # lg_handler.setFortmatter(lg_format)

    c_handler = logging.StreamHandler()
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)

    logger.addHandler(c_handler)
    logger.addHandler(lg_handler)

    return logger


def push_to_bigq(df, project, dataset, table, schema):
    # credentials from the service_account.json are not used
    # authentication is done automatically through the Google Cloud SDK
    # credentials = bigq_config()
    client = bigquery.Client(project=project)

    table_id = project + '.' + dataset + '.' + table
    if schema:
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_TRUNCATE')
    else:
        job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition='WRITE_TRUNCATE')
   
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    return job


def get_auth_token(api_name):
    client_token = os.environ.get('IS_CLIENT_TOKEN')
    if client_token:
        # parse string
        creds = Credentials.from_authorized_user_info(json.loads(client_token))
    else:
        # relative paths
        p_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
        cred_path = os.path.join(p_dir, 'credentials')
        client_secret = os.path.join(cred_path, 'client_secret.json')
        client_token =  os.path.join(cred_path, 'sheets.googleapis.com-python.json')
        if os.path.exists(client_token):
            creds = Credentials.from_authorized_user_file(client_token)
        else:
            gs_client = pygsheets.authorize(client_secret=client_secret, credentials_directory=cred_path)
            creds = Credentials.from_authorized_user_file(client_token)
    if api_name == 'sheets':
        service = build(api_name, 'v4', credentials=creds)
    else:
        service = build(api_name, 'v3', credentials=creds)
    return service


def get_spreadsheet_values(gsheet_id, sheet_range):
    retry_count = 10
    service = get_auth_token(api_name='sheets')
    values = None
    for n in range(0,10): # retry 10 times with exponential backoff
        try:
            # new method: gets only specific sheet. uses sheets api v4
            request = service.spreadsheets().values().batchGet(spreadsheetId=gsheet_id, ranges=[sheet_range])
            response = request.execute()
            values = response['valueRanges'][0]['values']
            break
        except googleapiclient.errors.HttpError as ex:
            # raise (f"HttpError: {ex}")
            # apply exponential backoff.
            time.sleep((2 ** n) + random.randint(0, 1000) / 1000)
    

    # old method: downloads entire file. uses the drive api v3
    # request = service.files().get_media(fileId=spreadsheet_id, range=sheet_range)
    # file_header = BytesIO()
    # downloader = MediaIoBaseDownload(file_header, request)
    # done = False
    # while done is False:
    #     status, done = downloader.next_chunk()
    #     print("Download %d%%." % int(status.progress() * 100))
    
    return values