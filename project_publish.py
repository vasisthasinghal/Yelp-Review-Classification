# sudo pip3 install google-cloud-pubsub

from google.cloud import pubsub_v1
import time
from google.cloud import storage
import ast

# Create Publisher
publisher = pubsub_v1.PublisherClient()

# Get Data
client = storage.Client()
bucket = client.get_bucket('project-bdl')
blob = bucket.get_blob('test.json')
data = blob.download_as_string()
rows = data.splitlines()
print('Downloaded Data')

# Publish Message
topic_name = 'projects/inspired-shell-288204/topics/to-kafka'

for row in rows:

    print('Publishing New Row...')

    row_str = row.decode('utf-8')
    row_dict = ast.literal_eval(row_str)
    row_dict = {key: str(value) for key, value in row_dict.items()}

    try:
        row_dict['text2'] = row_dict['text'][1024:]
    except:
        row_dict['text2'] = ""

    row_dict['text'] = row_dict['text'][:1024]

    publisher.publish(topic_name, data=b'None', **row_dict)
    time.sleep(45)
