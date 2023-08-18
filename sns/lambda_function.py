import boto3
import uuid
import json
import random
import logging
from datetime import datetime

logging.basicConfig(format="[%(levelname)s] [%(name)s] [%(asctime)s]: %(message)s",
                    level="INFO")
logger = logging.getLogger(__name__)

sns = boto3.client('sns', region_name='us-east-1')

def lambda_handler(event,context):
    # CREATE TOPIC
    topic_name = "example_sns_topic"
    create_response = sns.create_topic(Name=topic_name)
    topic_arn = create_response.get("TopicArn")
    logger.info("Create topic response: %s", create_response)


    # CREATE SUBSCRIPTIONS
    email_sub = sns.subscribe(TopicArn=topic_arn,
                          Protocol='email',
                          Endpoint="samim1000@gmail.com")
    phone_sub = sns.subscribe(TopicArn=topic_arn,
                          Protocol='sms',
                          Endpoint="+919437557886")
    sqs_sub = sns.subscribe(TopicArn=topic_arn,
                        Protocol='sqs',
                        Endpoint="arn:aws:sqs:us-east-1:291222035571:example_sqs_queue")

    # SEND MESSAGES
    sns.publish(TopicArn=topic_arn,
            Message=json.dumps(dict(item=random.randint(1, 100000),
                                    value=random.random(),
                                    sent_timestamp=datetime.utcnow().isoformat(),
                                    id=str(uuid.uuid4()))))
