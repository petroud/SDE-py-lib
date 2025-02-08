# src/kafka_async_client/producer.py

import asyncio
from aiokafka import AIOKafkaProducer

class KafkaProducer:
    def __init__(self, brokers: str):
        self.brokers = brokers
        self.producer = AIOKafkaProducer(bootstrap_servers=self.brokers)

    async def start(self):
        await self.producer.start()

    async def stop(self):
        await self.producer.stop()

    async def send(self, topic: str, value: dict):
        await self.producer.send_and_wait(topic, value.encode('utf-8'))
