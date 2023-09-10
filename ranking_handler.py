import boto3
import botocore
import os
import json
import uuid
from datetime import date, timedelta
from src.crawler import crawl_daily_ranking
from src.encoders.json_encoder import encode_ranking, encode_movie, decode_movie


# some global variables
BUCKET_NAME = os.environ["bucket_name"]
RANKING_FOLDER = os.environ["ranking_folder_prefix"]
MOVIES_FOLDER = os.environ["movies_folder_prefix"]
SQS_RANKING_QUEUE_URL = os.environ["sqs_ranking_queue_url"]
SQS_MOVIES_QUEUE_URL = os.environ["sqs_movies_queue_url"]
FILE_EXTENSION = os.environ["file_extension"]
UUID_NS_BASE = uuid.UUID(os.environ["uuid_ns_base"])

# sqs batch request allow maximum of 10 requests at the same time.
SQS_BATCH_LIMIT = 10

# Debugging
DEBUG = False

s3_client = boto3.client("s3")
sqs_clent = boto3.client("sqs")

# this value stores the oldest/smallest date has been seen/parsed.
# so the next date sent to SQS will be (next_date_to_crawl - timedelta(days=1))
next_date_to_crawl = None


def lambda_handler(event, context):
    global next_date_to_crawl, s3_client, sqs_clent
    if next_date_to_crawl == None:
        # a larger number here will always converge to the current date after comparison
        next_date_to_crawl = date.fromisoformat("2024-01-01")

    print(f"next_date_to_crawl = {next_date_to_crawl.isoformat()}")

    # Messages other than sent by EventBridge cron job are recurrent messages,
    # This will decide how many new messages will send to SQS.
    recurrent_messages = 0

    # merge all movie records by combining there revenues reocrd
    movies_seen = dict()
    message_deletion_request_entries = []

    message_list = event["Records"]

    # iterate through all crawl ranking requests
    for record in message_list:
        request_body = json.loads(record["body"])

        if "daily_ranking" not in request_body:
            # only message sent by EventBridge contains this field
            recurrent_messages += 1

        # parse paramter "date" to a date object
        d = date.fromisoformat(request_body["date"])

        # compare d with next_date_to_crawl
        if d < next_date_to_crawl:
            next_date_to_crawl = d

        print(f"start crawling move ranking on {d.isoformat()}")

        # use date as agrument to crawl the ranking
        movies = crawl_daily_ranking(d)

        my_print(f"Save rankings in S3 as {d.isoformat()}.{FILE_EXTENSION}")
        # convert List[Movie] to writable bytes in {FILE_EXTENSION} format
        writable = encode_ranking(movies)

        # Save to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{RANKING_FOLDER}/{d.isoformat()}.{FILE_EXTENSION}",
            Body=writable,
            ContentLanguage="en-US",
            ContentLength=len(writable),
        )
        print(f"Complete upload {d.isoformat()}.{FILE_EXTENSION}")

        # merge movies into movies_seen
        for movie in movies:
            if movie.id not in movies_seen:
                movies_seen[movie.id] = movie
            else:
                movies_seen[movie.id].merge_records(movie.revenues)

        # Use date as Id for deleting message in batch
        message_deletion_request = {
            "Id": str(uuid.uuid5(UUID_NS_BASE, record["messageId"])),
            "ReceiptHandle": record["receiptHandle"],
        }
        message_deletion_request_entries.append(message_deletion_request)

    sqs_clent.delete_message_batch(
        QueueUrl=SQS_RANKING_QUEUE_URL, Entries=message_deletion_request_entries
    )

    # Now, for each movie in movies_seen, check if {movie.id}.json existence by
    # examine the object header in S3 using list_objects_v2()
    #
    # If not exists (determined by S3.Client.exceptions.NoSuchKey exception),
    # simply create a new SQS message to fetch the full record of the movie
    #
    # If exists, compare the  metadata "newest_nth_day" with crawled movie's
    # largest nth_day. If lesser, no further operation is needed. Otherwise,
    # get the movie object, merge with new records, and put back to S3.
    sqs_movies_requests = []

    for movie in movies_seen.values():
        try:
            file_name = f"{MOVIES_FOLDER}/{movie.id}.{FILE_EXTENSION}"

            # retrieve object's metadata
            object_head_response = s3_client.head_object(
                Bucket=BUCKET_NAME, Key=file_name
            )

            nth_day = int(object_head_response["Metadata"]["newest-nth-day"])
            if nth_day >= movie.newest_nth_day():
                # no need to update the movie
                continue

            # fetch file from S3
            object_response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_name)

            # deserialize json into a Movie object
            movie_obj = decode_movie(object_response["Body"].read())

            # combine with crawled movie
            movie_obj.merge_records(movie.revenues)

            # put movie_obj back to S3
            writable = encode_movie(movie_obj)
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=writable,
                ContentLanguage="en-US",
                ContentLength=len(writable),
            )

        except botocore.exceptions.ClientError:
            # Movie {movie.title} does not exist in S3 yet,
            # construct a SQS request for crawl the detail
            my_print(
                f"Movie {movie.title} does not exist in S3, prepare new crawler job"
            )

            # prepare SQS message, with 0 second delay
            request = {
                "Id": movie.id,
                "MessageBody": json.dumps({"id": movie.id}),
            }
            sqs_movies_requests.append(request)

        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")

    # send_sqs_requests(SQS_MOVIES_QUEUE_URL, sqs_movies_requests)

    # handle recurrent requests for older day's ranking
    sqs_ranking_requests = []
    for i in range(recurrent_messages):
        next_date = next_date_to_crawl - timedelta(days=i + 1)
        request = {
            "Id": next_date.isoformat(),
            "MessageBody": json.dumps({"date": next_date.isoformat()}),
            "MessageGroupId": "movie-etl",
            # default delay is 15 minutes
        }
        sqs_ranking_requests.append(request)

    print(
        f"put in sqs request for rankings: {', '.join([entry['Id'] for entry in sqs_ranking_requests])}"
    )
    send_sqs_requests(SQS_RANKING_QUEUE_URL, sqs_ranking_requests)

    # end of execution
    return {"statusCode": 200}


# A wrapper for sending sqs requests
def send_sqs_requests(url, requests):
    global sqs_clent

    if len(requests) > 0:
        for n in range(0, len(requests), SQS_BATCH_LIMIT):
            entries = requests[n : n + SQS_BATCH_LIMIT]

            # response is dict type
            response = sqs_clent.send_message_batch(
                QueueUrl=url,
                Entries=entries,
            )

            # handle response result
            if "Failed" in response:
                for failed_message in response["Failed"]:
                    print(
                        f"requesst id={failed_message['Id']} failed, \
                            code={failed_message['Code']}, \
                            message={failed_message['Message']}"
                    )
            else:
                print(f"Messages are succussfully send to {url}")


def my_print(s):
    if DEBUG:
        print(s)
