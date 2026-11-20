import json
from datetime import datetime

import pytest
from pymongo.errors import DuplicateKeyError, PyMongoError

import main


class FakeMessage:
    """Records how the consumer settles a message."""

    def __init__(self, body: bytes, message_id: str = "msg-1"):
        self.body = body
        self.message_id = message_id
        self.outcome = None

    async def ack(self):
        self.outcome = ("ack",)

    async def reject(self, requeue=False):
        self.outcome = ("reject", requeue)

    async def nack(self, requeue=True):
        self.outcome = ("nack", requeue)


class FakeCollection:
    def __init__(self, error: Exception | None = None):
        self.error = error
        self.inserted = []

    async def insert_one(self, document):
        if self.error:
            raise self.error
        self.inserted.append(document)


def event(**overrides) -> bytes:
    payload = {"eventId": "evt-1", "type": "DEPOSIT", "accountNmbr": "acc-1", "amount": 10.5, **overrides}
    return json.dumps(payload).encode()


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    monkeypatch.setattr(main, "DB_RETRY_DELAY_SECONDS", 0)


async def test_valid_event_is_stored_with_event_id_and_acked():
    message, collection = FakeMessage(event()), FakeCollection()

    await main.process_and_save(message, collection)

    assert message.outcome == ("ack",)
    [stored] = collection.inserted
    assert stored["_id"] == "evt-1"
    assert stored["type"] == "DEPOSIT"
    assert stored["amount"] == 10.5
    assert isinstance(stored["receivedAt"], datetime)
    assert stored["receivedAt"].tzinfo is not None


async def test_legacy_message_without_event_id_is_stored_with_generated_id():
    message, collection = FakeMessage(json.dumps({"contenido": "Account created: 1"}).encode()), FakeCollection()

    await main.process_and_save(message, collection)

    assert message.outcome == ("ack",)
    assert "_id" not in collection.inserted[0]


@pytest.mark.parametrize("body", [b"not json", b"\xff\xfe", b"[1, 2]", b'"text"', b"42"])
async def test_malformed_messages_are_rejected_without_requeue(body):
    message, collection = FakeMessage(body), FakeCollection()

    await main.process_and_save(message, collection)

    assert message.outcome == ("reject", False)
    assert collection.inserted == []


async def test_duplicate_event_is_acked_without_error():
    message = FakeMessage(event())

    await main.process_and_save(message, FakeCollection(error=DuplicateKeyError("dup")))

    assert message.outcome == ("ack",)


async def test_database_errors_requeue_the_message():
    message = FakeMessage(event())

    await main.process_and_save(message, FakeCollection(error=PyMongoError("down")))

    assert message.outcome == ("nack", True)
