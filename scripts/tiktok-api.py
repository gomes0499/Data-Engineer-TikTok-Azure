import json
import time
import random
from faker import Faker
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
import configparser
from azure.identity import DefaultAzureCredential

account_url = "wu4eventhub-namespace.servicebus.windows.net"
default_credential = DefaultAzureCredential()

# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/4-Project/config/config.ini")

fake = Faker()

# Azure Event Hubs connection string
connection_str = config.get("eventhubs", "connection_str")
eventhub_name = config.get("eventhubs", "eventhub_name")
# fully_qualified_namespace = account_url
# credential = default_credential


# Number of videos and comments to generate
num_videos = 50
num_comments = 250

producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)

def send_event_hub_message(event_data):
    event = EventData(json.dumps(event_data))
    with producer:
        producer.send_batch([event])

# Generate and send video metadata
for _ in range(num_videos):
    video_data = {
        'event_type': 'video',
        'video_id': fake.uuid4(),
        'user_id': fake.uuid4(),
        'created_at': fake.date_time_this_year().isoformat(),
        'description': fake.sentence(),
        'music': fake.sentence(),
        'views': random.randint(1000, 1000000),
        'likes': random.randint(100, 100000),
        'comments_count': random.randint(10, 10000),
        'shares': random.randint(10, 50000),
        
    }

    send_event_hub_message(video_data)
    time.sleep(random.uniform(0.1, 1))  # Simulate streaming data with a random delay

# Generate and send comments
for _ in range(num_comments):
    video_id = fake.uuid4()  # Replace this with a random video_id from the video data you generated
    comment_data = {
        'event_type': 'comment',
        'comment_id': fake.uuid4(),
        'video_id': video_id,
        'user_id': fake.uuid4(),
        'comment_text': fake.sentence(),
        'created_at': fake.date_time_between_dates(datetime.now(), datetime.now()).isoformat(),
    }

    send_event_hub_message(comment_data)
    time.sleep(random.uniform(0.1, 1))  # Simulate streaming data with a random delay
