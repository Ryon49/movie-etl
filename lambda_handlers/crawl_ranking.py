import boto3
import os
import json
from datetime import date
from src.crawler import crawl_daily_ranking
from src.encoders.json_encoder import encode_ranking, encode_movie

from lambda_handlers.event_type import EventType
from lambda_handlers.aws_proxy import sns_publish

# some global variables
BUCKET_NAME = os.environ["bucket_name"]
RANKING_FOLDER = os.environ["ranking_folder_prefix"]
FILE_EXTENSION = os.environ["file_extension"]

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    
    # merge all movie records by combining there revenues reocrd
    movies_seen = dict()

    # List of string representing dates in ISO format
    dates = message["dates"]

    for date_string in dates:
        d = date.fromisoformat(date_string)
        ranking = crawl_daily_ranking(d)

        writable = encode_ranking(ranking)

        # Save to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{RANKING_FOLDER}/{date_string}.{FILE_EXTENSION}",
            Body=writable,
            ContentLanguage="en-US",
            ContentLength=len(writable),
        )

        # merge movies into movies_seen
        for movie in ranking:
            if movie.id not in movies_seen:
                movies_seen[movie.id] = movie
            else:
                movies_seen[movie.id].merge_records(movie.revenues)


    # Also, send a validation request back to controller to confirm crawler completion
    validation_request = {"event_type": EventType.VALIDATE_RANKING, "dates": dates}
    sns_publish(json.dumps(validation_request))
    
    # Finally, prepare another request for the downstream crawl_movie lambda
    # Convert movies to List[str]
    # movies = [encode_movie(movie).decode("utf-8") for movie in movies_seen.values()]

    # movie_detail_request = {
    #     "event_type": EventType.PREPARE_MOVIE_DETAIL,
    #     "movies": movies
    # }
    # sqs_client.send_message(
    #     QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(movie_detail_request)
    # )

    return {"statusCode": 200}
