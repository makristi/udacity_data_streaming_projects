"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "BROKER_URL": BROKER_URL,
            "SCHEMA_REGISTRY_URL": SCHEMA_REGISTRY_URL
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer({
                'bootstrap.servers': BROKER_URL,
                'group.id': 'km.avroconsumer',
                'schema.registry.url': SCHEMA_REGISTRY_URL})
        else:
            self.consumer = Consumer({
                'bootstrap.servers': BROKER_URL,
                'group.id': 'km.consumer',
                'auto.offset.reset': 'earliest'})
            pass

        logger.info(self.topic_name_pattern)
        
        self.consumer.subscribe([self.topic_name_pattern], on_assign = self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        logger.info("on_assign is incomplete - skipping")
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        result = 0
        message = self.poll(1.0)
        if message is None:
            logger.info("no message retrieved by consumer")
        elif message.error() is not None:
            logger.critical(f"consumer error {message.error()}")
        else:
            result = 1
        return result


    def close(self):
        """Cleans up any open kafka consumers"""
        self.close()
