import json
import boto3
import base64
dynamodb = boto3.resource('dynamodb')
table_name = dynamodb.Table('raw_views_data')


def lambda_handler(event, context):
    for record in event['Records']:
        pk_time = record["kinesis"]["partitionKey"]
        msg_decode = base64.b64decode(record["kinesis"]["data"]).decode('utf-8')
        dict_msg = json.loads(msg_decode)
        print(dict_msg)

        table_name.put_item(Item={
            "pk_time": pk_time,
            "data": dict_msg
        })

    return {
        'statusCode': 200
    }
