# py-wallet-audit

Consumes the account audit events that `java-wallet-account` publishes to RabbitMQ (`audit_queue`) and stores them in MongoDB. Events are stored once, keyed by `eventId`. Malformed messages are dropped, and messages are requeued while MongoDB is down.

## Run

```sh
python -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env   # fill in the RabbitMQ password
.venv/bin/python main.py
```

## Tests

```sh
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest              # all tests
.venv/bin/python -m pytest -m "not integration"   # skip the Docker-based test
```

`tests/test_integration.py` starts RabbitMQ and MongoDB with Testcontainers and is skipped when Docker is not available.
