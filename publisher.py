import json
import time
import boto3
from datetime import datetime

STREAM_NAME = "input-stream"
KINESIS_CLIENT = boto3.client('kinesis')


def get_views_data(interval_seconds: int = 5, stream_name=STREAM_NAME, kinesis_client=KINESIS_CLIENT):
    current_dt_value = datetime.now()
    dt_value = current_dt_value.strftime("%Y-%m-%d %H:%M:%S")

    with open("./resources/product-views.json") as lines:
        try:
            for line in lines:
                views_data = json.loads(line)
                print(views_data)

                kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(views_data),
                    PartitionKey=f"{hash(dt_value)}"
                )
                time.sleep(interval_seconds)  # Add a sleep

        except Exception as e:
            print(f"Error publishing message: {e}")


get_views_data()
