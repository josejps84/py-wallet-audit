"""Runs the real consumer against RabbitMQ and MongoDB containers."""

import asyncio
import json
import shutil
import subprocess

import aio_pika
import pytest
from pymongo import AsyncMongoClient

import main


def docker_available() -> bool:
    if not shutil.which("docker"):
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not docker_available(), reason="Docker is not available"),
]


@pytest.fixture(scope="module")
def containers():
    from testcontainers.community.mongodb import MongoDbContainer
    from testcontainers.community.rabbitmq import RabbitMqContainer

    with RabbitMqContainer("rabbitmq:4-alpine") as rabbit, MongoDbContainer("mongo:8") as mongo:
        rabbit_url = (f"amqp://{rabbit.username}:{rabbit.password}@{rabbit.get_container_host_ip()}:"
                      f"{rabbit.get_exposed_port(rabbit.port)}/")
        yield {
            "RABBITMQ_URL": rabbit_url,
            "MONGODB_URL": mongo.get_connection_url(),
            "MONGO_DB_NAME": "audit_test",
            "MONGO_COLLECTION": "audit_logs",
            "QUEUE_NAME": "audit_queue",
        }


async def publish(settings, *bodies: bytes):
    connection = await aio_pika.connect(settings["RABBITMQ_URL"])
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(settings["QUEUE_NAME"], durable=True)
        for body in bodies:
            await channel.default_exchange.publish(aio_pika.Message(body=body), routing_key=settings["QUEUE_NAME"])


async def wait_for(predicate, timeout=15.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not await predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.2)


async def test_events_are_stored_once_and_bad_messages_dropped(containers):
    stop = asyncio.Event()
    consumer = asyncio.create_task(main.consume(containers, stop))
    client = AsyncMongoClient(containers["MONGODB_URL"])
    collection = client[containers["MONGO_DB_NAME"]][containers["MONGO_COLLECTION"]]
    try:
        deposit = json.dumps({"eventId": "evt-1", "type": "DEPOSIT", "amount": 5}).encode()
        withdrawal = json.dumps({"eventId": "evt-2", "type": "WITHDRAWAL", "amount": 2}).encode()
        # evt-1 is delivered twice (e.g. a redelivery) and must be stored once
        await publish(containers, deposit, b"not json", withdrawal, deposit)

        async def both_stored():
            return await collection.count_documents({}) >= 2

        await wait_for(both_stored)
        await asyncio.sleep(1)  # give the duplicate time to be processed

        docs = await collection.find({}, {"_id": 1, "type": 1}).sort("_id").to_list()
        assert docs == [{"_id": "evt-1", "type": "DEPOSIT"}, {"_id": "evt-2", "type": "WITHDRAWAL"}]
    finally:
        stop.set()
        await asyncio.wait_for(consumer, timeout=10)
        await client.close()

    # Everything was settled: nothing left in the queue (the malformed one was rejected, not requeued)
    connection = await aio_pika.connect(containers["RABBITMQ_URL"])
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(containers["QUEUE_NAME"], durable=True)
        assert queue.declaration_result.message_count == 0
