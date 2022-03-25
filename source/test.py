from google.cloud import pubsub_v1
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
from pytz import timezone
import re

from utils import ( setup_logging, get_spreadsheet_values,
                    COLUMNS, push_to_bigq)

logger = setup_logging(Path(__file__).stem)
logger.debug('Executing. . .')

with pubsub_v1.SubscriberClient() as subscriber:
    # relevant GCP ids
    project_id = 'hub-data-295911'
    topic_id = 'incentives-data-pull'
    sub_id = 'incentives-data-pull-sub'
    topic_name = f'projects/{project_id}/topics/{topic_id}'
    subscription_name = f'projects/{project_id}/subscriptions/{sub_id}'
   
    # initialize request argument(s)
    request = {
        "subscription":subscription_name,
        "max_messages":1
    }
    # make the request
    response = subscriber.pull(request)
    # process messages in reqponse
    for msg in response.received_messages:
        attributes = dict(msg.message.attributes)
        try:
            # push to bigquery
            file_id = attributes['id']
            file_name = attributes['name']

            logger.debug(f"Pulling data for {file_name} ({file_id})")
            values = get_spreadsheet_values(gsheet_id=file_id, sheet_range='All Incentives')
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
            # TODO change dataset from dev to prod
            dataset = 'incentives_data_test'
            table = re.sub(r'\W+', '_', str(file_name).lower())
            job = push_to_bigq(df=data, project=project, dataset=dataset, table=table, schema=None)
            errors = job.result().errors
            logger.debug({'Errors':errors})

            # send ACK
            request = {
                "subscription":subscription_name,
                "ack_ids":[msg.ack_id]
            }
            subscriber.acknowledge(request)
            
        except KeyError as ex:
            continue

