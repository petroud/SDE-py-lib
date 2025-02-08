# tests/test_client.py

import asyncio
import conftest
from kafka_async_client.client import KafkaClient

@pytest.mark.asyncio
async def test_kafka_client():
    client = KafkaClient(
        brokers="localhost:9092",
        request_topic="request_topic",
        response_topic="response_topic",
        group_id="test_group"
    )

    await client.start()
    
    # Send a test request
    await client.send_request({"key": "value"})
    
    # Simulate receiving a response
    response = await client.get_response()
    
    assert response == {"key": "value"}
    
    await client.stop()
