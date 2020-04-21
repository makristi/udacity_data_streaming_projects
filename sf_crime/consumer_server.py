import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker



BROKER_URL = "PLAINTEXT://localhost:9092"


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        messages = c.consume(10, 1.0)
        for message in messages:
            if message is None:
                print("no message received by consumer")
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                print(f"consumed message {message.value()}")
        await asyncio.sleep(0.01)


if __name__ == "__main__":
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    
    try:
        asyncio.run(consume("udacity.com.km.sf.crime"))
    except KeyboardInterrupt as e:
        print("shutting down")

