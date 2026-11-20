"""Audit consumer: stores every account event published to RabbitMQ in MongoDB."""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone

import aio_pika
from dotenv import load_dotenv
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError, PyMongoError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("audit")

PREFETCH_COUNT = 10
DB_RETRY_DELAY_SECONDS = 2


def load_settings() -> dict[str, str]:
    """Reads the configuration and fails fast if anything is missing."""
    load_dotenv()
    required = ["RABBITMQ_URL", "MONGODB_URL", "MONGO_DB_NAME", "MONGO_COLLECTION", "QUEUE_NAME"]
    settings = {name: os.getenv(name, "") for name in required}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")
    return settings


async def process_and_save(message: aio_pika.abc.AbstractIncomingMessage, collection: AsyncCollection) -> None:
    """Decodes one message and stores it; acks, rejects or requeues it accordingly."""
    try:
        document = json.loads(message.body.decode())
        if not isinstance(document, dict):
            raise ValueError("payload is not a JSON object")
    except (UnicodeDecodeError, ValueError) as error:
        logger.error("Discarding malformed message %s: %s", message.message_id, error)
        # requeue=False: a malformed message would fail forever
        await message.reject(requeue=False)
        return

    # The event id makes redeliveries idempotent: the same event is stored only once
    if "eventId" in document:
        document["_id"] = document["eventId"]
    document["receivedAt"] = datetime.now(timezone.utc)

    try:
        await collection.insert_one(document)
        logger.info("Stored %s event %s", document.get("type", "unknown"), document.get("_id"))
    except DuplicateKeyError:
        logger.info("Event %s already stored, skipping duplicate", document.get("_id"))
    except PyMongoError as error:
        logger.error("MongoDB error, message will be retried: %s", error)
        # Back off before handing the message back so a down database isn't hammered
        await asyncio.sleep(DB_RETRY_DELAY_SECONDS)
        await message.nack(requeue=True)
        return

    await message.ack()


async def consume(settings: dict[str, str], stop: asyncio.Event) -> None:
    mongo_client: AsyncMongoClient = AsyncMongoClient(settings["MONGODB_URL"])
    collection = mongo_client[settings["MONGO_DB_NAME"]][settings["MONGO_COLLECTION"]]
    # Never process more messages at once than RabbitMQ hands us
    slots = asyncio.Semaphore(PREFETCH_COUNT)
    tasks: set[asyncio.Task] = set()

    async def handle(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        async with slots:
            await process_and_save(message, collection)

    try:
        await mongo_client.admin.command("ping")
        logger.info("Connected to MongoDB")

        # connect_robust reconnects automatically if the broker connection drops
        connection = await aio_pika.connect_robust(settings["RABBITMQ_URL"])
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=PREFETCH_COUNT)
            queue = await channel.declare_queue(settings["QUEUE_NAME"], durable=True)
            logger.info("Listening on queue '%s'", settings["QUEUE_NAME"])

            async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
                task = asyncio.create_task(handle(message))
                # Keep a reference so the task isn't garbage collected mid-flight
                tasks.add(task)
                task.add_done_callback(tasks.discard)

            consumer_tag = await queue.consume(on_message)
            await stop.wait()

            logger.info("Stopping: waiting for %d in-flight messages", len(tasks))
            await queue.cancel(consumer_tag)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await mongo_client.close()
        logger.info("Audit service stopped")


async def main() -> None:
    settings = load_settings()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await consume(settings, stop)


if __name__ == "__main__":
    asyncio.run(main())
