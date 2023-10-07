""" A utility to provide proxy calls to other AWS resources """
import boto3
import os

BUCKET_NAME = os.environ["bucket_name"]
SNS_TOPIC = os.environ["sns_topic"]

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def sns_publish(message: str):
    sns_client.publish(TopicArn=SNS_TOPIC, Message=message)

def s3_get_object(key: str) -> bytes:
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    return response["Body"].read()
