import json
import os
import boto3
from datetime import date, timedelta

from lambda_handlers.event_type import EventType
from lambda_handlers.aws_proxy import sns_publish

""" Lambda handler for controlling crawler events 

    The handler will receive two types of events: "new-ranking" and "old-ranking".
    The new-ranking event simply add 

"""
BUCKET_NAME = os.environ["bucket_name"]
CONFIGURATION_FILE = "metadata.json"


s3_client = boto3.client("s3")


def lambda_handler(event, context):
    message = json.loads(event["Records"][0]["Sns"]["Message"])

    event_type = message["event_type"]

    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=CONFIGURATION_FILE)
    crawler_config = json.loads(response["Body"].read())

    if event_type == EventType.RESET:
        crawler_config["next_date_to_crawl"] = "2023-10-04"
        crawler_config["validation_queue"] = []

    # Used as debugging, log the current config from metadata.json
    if event_type == EventType.DEBUG:
        print(crawler_config)
        return {"statusCode": 200}  # early stop, just print config, no need to write back to s3

    # Triggered once per day, append date (2 days before) into queue for the newest ranking
    # The ranking data for a given day usually won't be available until the next day,
    # So leave 2 days just to be save.
    if event_type == EventType.PREPARE_NEW_DATE:
        yesterday = date.today() - timedelta(days=2)
        yesterday_iso = yesterday.isoformat()
        # put the date to front so it will always get processed first
        crawler_config["ranking_queue"].insert(0, yesterday_iso)

    # Triggered every hour, it will prepare the "dates" parameter that consumed by crawl_ranking
    # length of "date" is capped by num_of_ranking_to_crawl
    if event_type == EventType.PREPARE_RANKING:
        num_of_ranking_to_crawl: int = crawler_config["num_of_ranking_to_crawl"]

        # combine validation_queue and ranking_queue
        # since each invoke of PREPARE_RANKING is set to >1 hour, retry thess dates if not validated with higher priority
        combined_list = (
            crawler_config["validation_queue"] + crawler_config["ranking_queue"]
        )

        # pick first "num_of_ranking_to_crawl" dates as payload
        target_dates = combined_list[:num_of_ranking_to_crawl]

        # target_dates has length less than num_of_ranking_to_crawl, add more dates
        next_date_to_crawl = date.fromisoformat(crawler_config["next_date_to_crawl"])
        while len(target_dates) < num_of_ranking_to_crawl:
            target_dates.append(next_date_to_crawl.isoformat())
            next_date_to_crawl = next_date_to_crawl - timedelta(days=1)

        payload = {"event_type": EventType.CRAWL_RANKING, "dates": target_dates}

        # publish the message
        sns_publish(json.dumps(payload))

        # set the future next_date_to_crawl
        crawler_config["next_date_to_crawl"] = next_date_to_crawl.isoformat()
        # remove target_dates from ranking_queue
        crawler_config["ranking_queue"] = combined_list[num_of_ranking_to_crawl:]
        # since validation_queue is combined with ranking_queue, new validation_queue will be the target_dates
        crawler_config["validation_queue"] = target_dates

    if event_type == EventType.VALIDATE_RANKING:
        validated_dates = message["dates"]

        # filter crawler_config
        crawler_config["validation_queue"] = [
            date
            for date in crawler_config["validation_queue"]
            if date not in validated_dates
        ]

    output = json.dumps(crawler_config)
    s3_client.put_object(
        Bucket=BUCKET_NAME, Key=CONFIGURATION_FILE, Body=output.encode("utf-8")
    )

    return {"statusCode": 200}
