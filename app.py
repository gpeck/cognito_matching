import base64
import json
import os
import certifi
import urllib3
from dotenv import load_dotenv
from function_cognito_match import matching_Algorithm

def handler(event, context):
    print('## start lambda')
    print(event)
    therecords = event['records']
    for record in therecords:
        rec = therecords[record]
        for record2 in rec:
            try:
                payload = base64.b64decode(record2["value"]).decode("utf-8")
                print("Decoded payload: " + str(payload))
                jsonpayload = json.loads(payload)
                final_1 = matching_Algorithm(
                    user_id=jsonpayload['user_id'],
                    first_name=jsonpayload['first_name'],
                    last_name=jsonpayload['last_name'],
                    date_of_birth=jsonpayload['date_of_birth'],
                    street=jsonpayload['street'],
                    zipcode=jsonpayload['zipcode'],
                    phone=jsonpayload['phone'])
                result_data = final_1.result()
                data = json.dumps(result_data)
                print("Cognito Matching Algorithm Result: " + data)
                load_dotenv()
                url = os.getenv("API_URL")
                key = os.getenv("API_X_KEY")
                if "No match found" in data:
                    data = json.dumps({"user_id": final_1.user_id, "name_DOB": "",
                                       "street_address": "",
                                       "email": "",
                                       "Matched_Household": "",
                                       "Fraud_same_person": "",
                                       "Fraud_household": ""})
                fields = {
                    "x-api-key": key,
                    "response": data,
                }
                http = urllib3.PoolManager(ca_certs=certifi.where())
                last_status = http.request("POST", url, fields)
                print(json.loads(last_status.data.decode("utf-8"))['message'])
            except Exception as e:
                error_message = f"Unexpected error: {e}"
                print(error_message)
    return {
        'statusCode': 200,
        'body': data,
        'apiResponseStatus': last_status.status,
        'apiMessage': json.loads(last_status.data.decode("utf-8"))['message']
    }
