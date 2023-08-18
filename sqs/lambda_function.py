import boto3
import uuid
import json
import random
import logging
from datetime import datetime

logging.basicConfig(format="[%(levelname)s] [%(name)s] [%(asctime)s]: %(message)s",
                    level="INFO")
logger = logging.getLogger(__name__)

sqs = boto3.client('sqs', region_name='us-east-1')

def lambda_handler(event,context):
    # CREATE QUEUE
    queue_name = "example_sqs_queue"
    create_response = sqs.create_queue(QueueName=queue_name,
                                   # timeout in seconds -> int from 0 to 43,200 (12 hours)
                                   Attributes={'VisibilityTimeout': '3600'})
    queue_url = create_response.get("QueueUrl")
    logger.info("Create queue response: %s", create_response)

    # SEND MESSAGES
    attributes = {'sent_to_sqs_utc_date': {'DataType': 'String',
                                       'StringValue': datetime.utcnow().isoformat()}}
    send_response = sqs.send_message(QueueUrl=queue_url,
                                 MessageAttributes=attributes,
                                 MessageBody=json.dumps(dict(item=random.randint(1, 100000),
                                                             value=random.random(),
                                                             id=str(uuid.uuid4()))))
    logger.info("Send message response: %s", send_response)

    # PROCESS MESSAGES
    received_msg = sqs.receive_message(QueueUrl=queue_url,
                                   MaxNumberOfMessages=10,
                                   MessageAttributeNames=['All'],
                                   AttributeNames=['All'])

    retrieved_messages = []
    if 'Messages' in received_msg and len(received_msg['Messages']) >= 1:
        for message in received_msg['Messages']:
            full_msg = message['Body']
            retrieved_messages.append(json.loads(full_msg))
            receipt_handle = message['ReceiptHandle']
            del_msg_resp = sqs.delete_message(QueueUrl=queue_url,
                                          # md5 hashed identifier of message body
                                          ReceiptHandle=receipt_handle)
            logger.info("Deleted message from the queue with receipt handle %s. Response: %s",
                    receipt_handle,
                    del_msg_resp)

    logger.info("Processed messages: %s", retrieved_messages)
