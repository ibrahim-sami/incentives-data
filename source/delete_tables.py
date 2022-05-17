from pathlib import Path
import re
from google.cloud import bigquery
from numpy import exp

from utils import get_auth_token, setup_logging

def delete_tables():
    logger = setup_logging(Path(__file__).stem)
    logger.debug('Deleting tables for files that were removed or renamed on the root folder')
    '''
        Get list of files from root folder
    '''
    root_folder_url = 'https://drive.google.com/drive/u/0/folders/1BNDA99Ca6X7Q1QQX_PPwIDQqm26WfJvy'
    root_folder_id = root_folder_url.split('/')[-1]
    logger.debug(f"Pulling list of files from rooter folder: {root_folder_url}")
    folder_type = 'application/vnd.google-apps.folder'

    query = f"parents = '{root_folder_id}' and mimeType='{folder_type}'"

    # Call the Drive v3 API
    service = get_auth_token(api_name='drive')
    response = service.files().list(q=query).execute()

    folders = response.get('files', [])
    logger.debug(f'{len(folders)} folders found')

    files = []
    file_type = 'application/vnd.google-apps.spreadsheet'
    for folder in folders:
        logger.debug(f"Pulling list of files from folder: {folder['name']}")
        id = folder['id']
        query = f"parents='{id}' and mimeType='{file_type}'"
        response = service.files().list(q=query, fields='files(id, name)').execute()
        files.extend(response.get('files', []))
    logger.debug(f'{len(files)} found')

    logger.debug('Processing file names to match expected tables names')
    expected_bigq_tables = []
    for f in files:
        table_name = re.sub(r'\W+', '_', str(f['name']).lower())
        expected_bigq_tables.append(table_name)

    '''
        Get bigquery list of tables
    '''
    client = bigquery.Client()
    dataset_id = 'hub-data-295911.incentives_data'
    logger.debug(f'Pulling list of tables already on Bigquery dataset: {dataset_id}')
    tables = client.list_tables(dataset_id)  # Make an API request.
    print("Tables contained in '{}':".format(dataset_id))
    bigq_tables = []
    for table in tables:
        bigq_tables.append(str(table.table_id))
    logger.debug(f'{len(bigq_tables)} found')

    tables_to_del = []
    for table in bigq_tables:
        if table not in expected_bigq_tables:
            tables_to_del.append(table)
            logger.debug(f"File not found: {table}")

            table_id = f'hub-data-295911.incentives_data.{table}'

            # If the table does not exist, delete_table raises
            # google.api_core.exceptions.NotFound unless not_found_ok is True.
            client.delete_table(table_id, not_found_ok=True)  # Make an API request.
            logger.debug("Deleted corresponging table '{}'.".format(table_id))

    return tables_to_del

