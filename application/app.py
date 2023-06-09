import json
import boto3
import datetime
# import requests


def lambda_handler(event, context):
    record = {}
    
    for eventRecord in event["Records"]:
        record = parserRecord(eventRecord)
        addHistoryRow(record)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
        }),
    }

def parserRecord(record):
    result = {}
    eventName = record['eventName']
    
    result['eventName'] = eventName
    result['id'] = record['dynamodb']['Keys']['id']["S"]
    result['customerId'] = record['dynamodb']['Keys']['customerId']["S"]
    result['date'] = datetime.date.today().strftime('%Y-%m-%d %H:%M:%S')
    
    if eventName == 'REMOVE' or eventName == 'MODIFY':
        result['document'] = record['dynamodb']['OldImage']['document']["S"]
        result['name'] = record['dynamodb']['OldImage']['name']["S"]
    else:
        result['document'] = record['dynamodb']['NewImage']['document']["S"]
        result['name'] = record['dynamodb']['NewImage']['name']["S"]
    
    return result

def addHistoryRow(row):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('dev-history-table')

    response = table.put_item(
        Item=row
    )