import json
import boto3
import datetime
# import requests


def lambda_handler(event, context):
    record = {}
    
    for eventRecord in event["Records"]:
        record = parserRecord(eventRecord)
        # addHistoryRow(record)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
        }),
    }

def parserRecord(record):
    image = {}
    result = {}

    eventName = record['eventName']

    if eventName == 'REMOVE' or eventName == 'MODIFY':
        image = record['dynamodb']['OldImage']
    else:
        image = record['dynamodb']['NewImage']

    for key, value in image.items():
        result[key] = value[list(value.keys())[0]]

    result['eventName'] = eventName
    result['data'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print(result)

    return result

def addHistoryRow(row):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('dev-history-table')

    response = table.put_item(
        Item=row
    )