# src/kafka_async_client/client.py

import asyncio
from .producer import KafkaProducer
from .consumer import KafkaConsumer

class KafkaClient:
    def __init__(self, brokers: str, request_topic: str, response_topic: str, group_id: str):
        self.brokers = brokers
        self.request_topic = request_topic
        self.response_topic = response_topic
        self.group_id = group_id
        self.producer = KafkaProducer(brokers)
        self.consumer = KafkaConsumer(brokers, response_topic, group_id)

    async def start(self):
        await self.producer.start()
        await self.consumer.start()

    async def stop(self):
        await self.producer.stop()
        await self.consumer.stop()

    async def send_request(self, request: dict):
        await self.producer.send(self.request_topic, request)

    async def get_response(self):
        async for response in self.consumer.consume():
            return response
