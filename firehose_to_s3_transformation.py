import base64
import json
import ast

output = []


def lambda_handler(event, context):
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        print('payload:', payload)  

        payload_str = json.loads(payload)

        payload_dict = ast.literal_eval(payload_str)
        print('payload_dict', type(payload_dict))    # dict to parsing

        msg = {
            "event": payload_dict.get('event', ''),
            "messageid": payload_dict.get('messageid', ''),
            "userid": payload_dict.get('userid', ''),
            "productid": payload_dict.get('properties', {}).get('productid', ''),
            "source": payload_dict.get("context", {}).get("source", '')
        }

        payload_str = json.dumps(msg) # str to add new_line
        print('payload_str:', payload_str)
        row_w_newline = payload_str + "\n"
        row_w_newline_encoded = base64.b64encode(row_w_newline.encode('utf-8'))

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': row_w_newline_encoded
        }
        output.append(output_record)

    print('Processed {} records.'.format(len(event['records'])))

    return {'records': output}
