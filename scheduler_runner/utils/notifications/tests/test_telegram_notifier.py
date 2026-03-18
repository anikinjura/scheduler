import requests

from scheduler_runner.utils.notifications.implementations.telegram_notifier import TelegramNotifier


class DummyLogger:
    def trace(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def build_notifier(**overrides):
    config = {
        "TELEGRAM_BOT_TOKEN": "token",
        "TELEGRAM_CHAT_ID": "chat-id",
        "SEND_RETRY_ATTEMPTS": 3,
        "SEND_RETRY_BACKOFF_SECONDS": 0,
        **overrides,
    }
    return TelegramNotifier(config=config, logger=DummyLogger())


def test_connect_does_not_call_remote_preflight(monkeypatch):
    notifier = build_notifier()

    def fail_get(*args, **kwargs):
        raise AssertionError("requests.get must not be called from connect()")

    monkeypatch.setattr(requests, "get", fail_get)

    assert notifier.connect() is True


def test_internal_send_retries_on_timeout(monkeypatch):
    notifier = build_notifier(SEND_RETRY_ATTEMPTS=2, SEND_RETRY_BACKOFF_SECONDS=0)
    attempts = {"count": 0}

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {"ok": True, "result": {"message_id": 1}}

    def fake_post(*args, **kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise requests.exceptions.Timeout("slow network")
        return Response()

    monkeypatch.setattr(requests, "post", fake_post)

    success, result = notifier._internal_send_telegram_message("token", "chat-id", "message")

    assert success is True
    assert result["ok"] is True
    assert attempts["count"] == 2


def test_internal_send_does_not_retry_non_retryable_http(monkeypatch):
    notifier = build_notifier(SEND_RETRY_ATTEMPTS=3, SEND_RETRY_BACKOFF_SECONDS=0)
    attempts = {"count": 0}

    class Response:
        status_code = 400
        text = "bad request"

        @staticmethod
        def json():
            return {"ok": False, "description": "Bad Request"}

    def fake_post(*args, **kwargs):
        attempts["count"] += 1
        return Response()

    monkeypatch.setattr(requests, "post", fake_post)

    success, result = notifier._internal_send_telegram_message("token", "chat-id", "message")

    assert success is False
    assert result["ok"] is False
    assert attempts["count"] == 1
