import os
import re
from pathlib import Path
from urllib.error import HTTPError
from pytz import timezone
from datetime import datetime
import pandas as pd

from utils import (setup_logging, get_spreadsheet_values, 
                    push_to_bigq, COLUMNS)

def execute(event, context):
    logger = setup_logging(Path(__file__).stem)
    logger.debug('Executing. . .')

    if event['attributes']:
        try:
            file_id = event['attributes']['id']
            file_name = event['attributes']['name']

            logger.debug(f"Pulling data for {file_name} ({file_id})")
            try:
                values = get_spreadsheet_values(gsheet_id=file_id, sheet_range='All Incentives')
            except HTTPError as ex:
                raise (f"HttpError: {ex}")
            data = values[1:]
            data = pd.DataFrame(data)
            # keep first 10 columns
            data = data.iloc[:, : 10]
            data.columns = COLUMNS
            data.dropna(inplace=True)
            logger.debug('%s records pulled', str(data.shape[0]))

            p_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
            output_path = os.path.join(p_dir, 'output', 'sample.csv')
            # data.to_csv(output_path)

            data['ingestion_timestamp'] = datetime.now(tz=timezone('Africa/Nairobi'))

            logger.debug(f"Pushing data to big query for {file_name} ({file_id})")
            project = 'hub-data-295911'
            dataset = 'incentives_data'
            table = re.sub(r'\W+', '_', str(file_name).lower())
            job = push_to_bigq(df=data, project=project, dataset=dataset, table=table, schema=None)
            errors = job.result().errors
            logger.debug({'Errors':errors})
        except KeyError as e:
            raise Exception("KeyError. Invalid attributes supplied.")
    else:
        raise Exception('KeyError. No attributes supplied.')


if __name__ == "__main__":
    execute(None, None)

    # for testing 
    # p_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
    # sample_file_list = os.path.join(p_dir, 'output', 'sample_file_list.txt')
    # with open(sample_file_list, 'r', encoding='utf-8') as f:
    #     lines = f.readlines()
    #     for line in lines:
    #         line = str(line).replace("\'", "\"")
    #         print(line)
    #         file = json.loads(line)
    #         file_id = file['id']
    #         file_name = file['name']

    #         event = dict(
    #             attributes=dict(
    #             id=file_id,
    #             name=file_name
    #         ))

    #         execute(event, None)

        


    