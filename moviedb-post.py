import json
import boto3
import requests
import os
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    OMDB = os.environ["OMDBkey"]
    
    dynamodb = boto3.resource('dynamodb') # may require parameters if not using default AWS environment vars
    
    table = dynamodb.Table('movies')
    
    movie = requests.get("https://www.omdbapi.com/?apikey=" + OMDB + "&t=" + event["queryStringParameters"]["title"] + "&y=" + event["queryStringParameters"]["year"])
    
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
        'body' : out["title"]
        }
    elif movie.json()["Response"] == "False" and movie.json()["Error"] == "Daily request limit reached!":
        return{
            'statusCode': 429,
            'body': "OMDB API request limit reached!"
        }
    elif movie.json()["Response"] == "False" and movie.json()["Error"] == "Movie not found!":
        return{
            'statusCode': 404,
            'body': "Movie Not Found!"
        }
    else:
        return{
            'statusCode': 520,
            'body': "Unknown Error!"
        }