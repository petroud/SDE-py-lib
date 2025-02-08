# src/kafka_async_client/consumer.py

import asyncio
from aiokafka import AIOKafkaConsumer

class KafkaConsumer:
    def __init__(self, brokers: str, topic: str, group_id: str):
        self.brokers = brokers
        self.topic = topic
        self.group_id = group_id
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.brokers,
            group_id=self.group_id
        )

    async def start(self):
        await self.consumer.start()

    async def stop(self):
        await self.consumer.stop()

    async def consume(self):
        async for message in self.consumer:
            yield message.value.decode('utf-8')
