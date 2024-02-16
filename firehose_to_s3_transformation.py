import base64
import json
import ast

FIREHOSE_STREAM_NAME = "views_data_transformation"
S3_BUCKET = "processedviewsdata"

output = []


def lambda_handler(event, context):
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        print('payload:', payload)  # "{\"event\": \"ProductView\", \"messageid\": \"6b1291ea-e50d-425b-9940-44c2aff089c1\", \"userid\": \"user-78\", \"properties\": {\"productid\": \"product-173\"}, \"context\": {\"source\": \"desktop\"}}\n"

        payload_str = json.loads(payload)
        print('payload_dict:', payload_str)
        print('payload_dict', type(payload_str))  # string

        payload_dict = ast.literal_eval(payload_str)
        print('payload_dict', type(payload_dict))  # dict to parse

        msg = {
            "event": payload_dict.get('event', ''),
            "messageid": payload_dict.get('messageid', ''),
            "userid": payload_dict.get('userid', ''),
            "productid": payload_dict.get('properties', {}).get('productid', ''),
            "source": payload_dict.get("context", {}).get("source", '')
        }
        print(msg)
        print('msg', type(msg))

        payload_str = json.dumps(msg)
        print('payload_str:', payload_str)

        row_w_newline = payload_str + "\n"
        print('row_w_newline type:', type(row_w_newline))

        row_w_newline_encoded = base64.b64encode(row_w_newline.encode('utf-8'))
        print('row_w_newline_encoded', type(row_w_newline_encoded))

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': row_w_newline_encoded
        }
        output.append(output_record)

    print('Processed {} records.'.format(len(event['records'])))

    return {'records': output}

