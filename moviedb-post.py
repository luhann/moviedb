import json
import boto3
from botocore.vendored import requests
import os
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    OMDB = os.environ["OMDBkey"]
    
    dynamodb = boto3.resource('dynamodb') # may require parameters if not using default AWS environment vars
    
    table = dynamodb.Table('movies')
    
    movie = requests.get("http://www.omdbapi.com/?apikey=" + OMDB + "&t=" + event["queryStringParameters"]["movie"] + "&y=" + event["queryStringParameters"]["year"])
    
    if movie.json()["Response"] == "True":
        out = {}

        for key in movie.json():
            
            if key == "Ratings":
                out[key.lower()] = movie.json()[key]
                out[key.lower()].append({"Source": "Personal", "Value": event["queryStringParameters"]["rating"]}) 
            else:
              out[key.lower()] = movie.json()[key]
        try:
            del out['response']
        except KeyError:
            pass
            
        table.put_item(Item = out)
        
        return {
        'statusCode': 200,
        }
    else:
        return {
            'statusCode': 500,
            "error": movie.json()['Error']
            
        }
    
    
    
    