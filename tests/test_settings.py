import pytest

import main

REQUIRED = ["RABBITMQ_URL", "MONGODB_URL", "MONGO_DB_NAME", "MONGO_COLLECTION", "QUEUE_NAME"]


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch):
    # Never read the developer's .env during tests
    monkeypatch.setattr(main, "load_dotenv", lambda: None)
    for name in REQUIRED:
        monkeypatch.delenv(name, raising=False)


def test_all_variables_present(monkeypatch):
    for name in REQUIRED:
        monkeypatch.setenv(name, f"value-{name}")

    assert main.load_settings() == {name: f"value-{name}" for name in REQUIRED}


def test_missing_variables_stop_the_service(monkeypatch):
    monkeypatch.setenv("RABBITMQ_URL", "amqp://localhost")
    monkeypatch.setenv("QUEUE_NAME", "")

    with pytest.raises(SystemExit) as exit_info:
        main.load_settings()

    message = str(exit_info.value)
    for name in ["MONGODB_URL", "MONGO_DB_NAME", "MONGO_COLLECTION", "QUEUE_NAME"]:
        assert name in message
    assert "RABBITMQ_URL" not in message
