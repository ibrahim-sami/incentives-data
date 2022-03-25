from pathlib import Path

from utils import get_auth_token, setup_logging

def execute(event, context):
    logger = setup_logging(Path(__file__).stem)
    logger.debug('Executing. . .')

    logger.debug('Pulling list of folders from root folder')
    root_folder_url = 'https://drive.google.com/drive/u/0/folders/1BNDA99Ca6X7Q1QQX_PPwIDQqm26WfJvy'
    root_folder_id = root_folder_url.split('/')[-1]
    folder_type = 'application/vnd.google-apps.folder'

    query = f"parents = '{root_folder_id}' and mimeType='{folder_type}'"

    # Call the Drive v3 API
    service = get_auth_token(api_name='drive')
    response = service.files().list(q=query).execute()

    folders = response.get('files', [])
    logger.debug(f'{len(folders)} found: {folders}')

    
    files = []
    file_type = 'application/vnd.google-apps.spreadsheet'
    for folder in folders:
        logger.debug(f"Pulling list of files from folder: {folder['name']}")
        id = folder['id']
        query = f"parents='{id}' and mimeType='{file_type}'"
        response = service.files().list(q=query, fields='files(id, name)').execute()
        files.extend(response.get('files', []))

    logger.debug('Pushing file ids to Pub/Sub queue')
    for f in files:
        print(f)

if __name__ == "__main__":
    execute(None, None)