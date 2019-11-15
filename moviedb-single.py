import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

# always start with the lambda_handler


def lambda_handler(event, context):

    # make the connection to dynamodb
    # may require parameters if not using default AWS environment vars
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('movies')
    # get item from database
    items = table.get_item(
        Key={"title": event["queryStringParameters"]["title"], "year": event["queryStringParameters"]["year"]})
    items = items['Item']

    return {
        'statusCode': 200,
        'body': json.dumps(items)
    }
