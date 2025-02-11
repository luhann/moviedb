import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb') # may require parameters if not using default AWS environment vars
    
    table = dynamodb.Table('movies')
    
    response = table.scan()

    items = []
    
    for i in response['Items']:
        items.append(i)
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
            )
    
        for i in response['Items']:
            items.append(i)
        
    return {
        'statusCode': 200,
        'body': json.dumps(items)
    }