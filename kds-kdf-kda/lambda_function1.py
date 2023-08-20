import json
import requests
import boto3
import uuid
import time
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = boto3.client('kinesis', region_name='us-east-1')

# Convert a UUID to a string of hex digits in standard form
#If all you want is a unique ID, you should probably call uuid1() or uuid4()
partition_key = str(uuid.uuid4())

def lambda_handler(event, context):
    while True:               # make infinite Loop
        send_data_record()
        time.sleep(10)
        
def send_data_record():
    url = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms=ETH,DASH&tsyms=BTC,USD,EUR&api_key=c4edf2adee28e331204d85bb499639f28dd99dba2201c5ecc3e6354c76cf3c4f'
    r = requests.get(url)
    #we have used json() method which prints the JSON data in the Python dictionary format
    data = r.json() 
    BTC = data['ETH']['BTC']
    USD = data['ETH']['USD']
    EUR = data['ETH']['EUR']
    
    
    try:
        response = client.put_record(
                StreamName='crypto_prices',
                Data=json.dumps(data),
                PartitionKey=partition_key)
        logger.info("Put record in stream %s.", 'crypto_prices')
    except ClientError:
        logger.exception("Cloud not put record in stream %s", 'crypto_prices')
        raise
    else:
        return response
    
    
        

    
    
