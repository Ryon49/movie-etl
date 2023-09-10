import os
import json
import boto3

from src.crawler import crawl_movie_detail
from src.encoders.json_encoder import encode_movie

BUCKET_NAME = os.environ["bucket_name"]
MOVIES_FOLDER = os.environ["movies_folder_prefix"]
FILE_EXTENSION = os.environ["file_extension"]


def lambda_handler(event, context):
    s3_client: boto3.S3.Client = boto3.client("s3")

    records = event["Records"]
    for record in records:
        request_body = json.loads(record["body"])

        # use for constructing target URL
        movie_id = request_body["id"]

        print(f"start crawling movie with id {movie_id}")

        # use movie id to fetch detail from boxofficedojo.com
        movie = crawl_movie_detail(movie_id)

        # convert Movie object into writable bytes in {FILE_EXTENSION} format
        writable = encode_movie(movie)

        file_name = f"{MOVIES_FOLDER}/{movie_id}.{FILE_EXTENSION}"
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=writable,
            ContentLanguage="en-US",
            ContentLength=len(writable),
            Metadata={"newest-nth-day": str(movie.newest_nth_day())},
        )

        print(f"Complete crawling {movie_id}")

    return {
        "statusCode": 200,
    }
