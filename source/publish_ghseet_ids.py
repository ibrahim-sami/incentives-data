from pathlib import Path
from typing import final
from google.cloud import pubsub_v1

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
    logger.debug(f'{len(folders)} folders found')

    files = []
    file_type = 'application/vnd.google-apps.spreadsheet'
    for folder in folders:
        logger.debug(f"Pulling list of files from folder: {folder['name']}")
        id = folder['id']
        query = f"parents='{id}' and mimeType='{file_type}'"
        response = service.files().list(q=query, fields='files(id, name)').execute()
        files.extend(response.get('files', []))

    project_id = 'hub-data-295911'
    topic_id = 'incentives-data-pull'
    with pubsub_v1.PublisherClient() as publisher:
        topic_path = publisher.topic_path(project_id, topic_id)
        logger.debug(f'Pushing file ids to Pub/Sub topic {topic_path}')
        results = []
        # TODO remove list limit for testing
        for f in files[0:3]:
            logger.debug(f"Filename: {f['name']} FileID: {f['id']} pushed")
            future = publisher.publish( topic=topic_path, 
                                        data=b'Incentives data file', 
                                        id=f['id'], 
                                        name=f['name']
                                        )
            try:
                future.result()
            except Exception as ex:
                future.cancel()
                raise ex
        
        return results

if __name__ == "__main__":
    execute(None, None)