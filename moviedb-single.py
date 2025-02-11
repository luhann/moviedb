import json
import boto3
import requests
import os
from boto3.dynamodb.conditions import Key, Attr


# always start with the lambda_handler
def lambda_handler(event, context):

    OMDB = os.environ["OMDBkey"]

    # make the connection to dynamodb
    dynamodb = boto3.resource(
        "dynamodb"
    )  # may require parameters if not using default AWS environment vars

    table = dynamodb.Table("movies")

    items = table.get_item(
        Key={
            "title": event["queryStringParameters"]["title"],
            "year": event["queryStringParameters"]["year"],
        }
    )

    if "Item" in items:
        # get item from database
        return {"statusCode": 200, "body": json.dumps(items["Item"])}
    else:
        return {"statusCode": 404, "body": "Movie Not Found!"}
