import json
import boto3
from botocore.vendored import requests
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

    movie = requests.get(
        "https://www.omdbapi.com/?apikey="
        + OMDB
        + "&t="
        + event["queryStringParameters"]["title"]
        + "&y="
        + event["queryStringParameters"]["year"]
    )

    if movie.json()["Response"] == "True":

        # get item from database
        items = table.get_item(
            Key={"title": movie.json()["Title"], "year": movie.json()["Year"]}
        )
        return {"statusCode": 200, "body": json.dumps(items["Item"])}
    elif (
        movie.json()["Response"] == "False"
        and movie.json()["Error"] == "Daily request limit reached!"
    ):
        return {"statusCode": 429, "body": "OMDB API request limit reached!"}
    elif (
        movie.json()["Response"] == "False"
        and movie.json()["Error"] == "Movie not found!"
    ):
        return {"statusCode": 404, "body": "Movie Not Found!"}
    else:
        return {"statusCode": 520, "body": "Unknown Error!"}
