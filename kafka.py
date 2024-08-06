import json
import logging
import sys
import time

from bson import ObjectId
from confluent_kafka import Consumer, KafkaError
from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection as MongoCollection
from pymongo.server_api import ServerApi

logger: logging.Logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
    
def setup_kafka(topic: str) -> Consumer:
    # Configure the consumer
    uri_prefix = topic.removeprefix('user-consumable-')
    conf: dict = {
        'bootstrap.servers': f'{uri_prefix}-bootstrap.streaming.ditto.live:443',
        'security.protocol': 'SSL',
        'ssl.ca.location': 'cluster.cert',
        'ssl.certificate.location': 'user.cert',
        'ssl.key.location': 'user.key',
        'group.id': topic,
        'auto.offset.reset': 'earliest'
    }
    
    # Create the consumer
    consumer: Consumer = Consumer(conf, logger=logger)
    logger.info("Created the Ditto Kafka consumer")

    # Subscribe to the topic
    consumer.subscribe([topic])
    logger.info(f"Subscribed to the topic '{topic}'")
    
    return consumer

def setup_mongo(uri: str) -> MongoCollection:
    # Create a new client and connect to the server
    client: MongoClient = MongoClient(uri, server_api=ServerApi('1'))

    client.admin.command('ping')
    logger.info("Pinged your deployment. You successfully connected to MongoDB!")

    # Change these
    db = 'matt-db'
    coll = 'ditto'
    mdb = client[db][coll]
    logger.info(f"Connected to collection '{coll}' in db '{db}', found {mdb.count_documents({})} docs")
    return mdb

def main():
    mongo_uri = sys.argv[1]
    kafka_topic = sys.argv[2]
    consumer = setup_kafka(kafka_topic)
    mdb = setup_mongo(mongo_uri)
    
    # Read messages from the topic
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                break
            else:
                # Error occurred
                logger.error(f"Error: {msg.error().str()}")
                continue

        # Process the message
        value: str = msg.value().decode('utf-8')
        change: dict = json.loads(value)
        logger.info(f"Handling change: {json.dumps(change, indent=2)}")
        if change['type'] == 'documentChanged':
            # There is a "gotcha" here in case the Ditto customer uses a composite ID field
            # Don't believe there's an equivalent in MongoDB, so you'd have to handle that
            # Read more in https://docs.ditto.live/document-model/identifiers#wy82F 
            new_value: dict = change['change']['newValue']
            new_value['_id'] = ObjectId(new_value['_id'])
            new_value.pop('_version')
            mdb.insert_one(new_value)
        
        # Sleeping just for testing purposes, not needed in practice
        time.sleep(1)
        

    # Close the consumer
    consumer.close()

if __name__ == "__main__":
    main()