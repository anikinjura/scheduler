import requests

from scheduler_runner.utils.notifications.implementations.vk_notifier import VkNotifier


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
        "VK_ACCESS_TOKEN": "vk-token",
        "VK_PEER_ID": "2000000001",
        "SEND_RETRY_ATTEMPTS": 3,
        "SEND_RETRY_BACKOFF_SECONDS": 0,
        **overrides,
    }
    return VkNotifier(config=config, logger=DummyLogger())


def test_connect_does_not_call_remote_preflight(monkeypatch):
    notifier = build_notifier()

    def fail_post(*args, **kwargs):
        raise AssertionError("requests.post must not be called from connect()")

    monkeypatch.setattr(requests, "post", fail_post)

    assert notifier.connect() is True


def test_internal_send_retries_on_timeout(monkeypatch):
    notifier = build_notifier(SEND_RETRY_ATTEMPTS=2, SEND_RETRY_BACKOFF_SECONDS=0)
    attempts = {"count": 0}

    def fake_post(*args, **kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise requests.exceptions.Timeout("slow network")

        class Response:
            @staticmethod
            def raise_for_status():
                return None

            @staticmethod
            def json():
                return {"response": 5}

        return Response()

    monkeypatch.setattr(requests, "post", fake_post)

    success, result = notifier._internal_send_vk_message("message")

    assert success is True
    assert result["response"] == 5
    assert attempts["count"] == 2


def test_internal_send_does_not_retry_non_retryable_vk_error(monkeypatch):
    notifier = build_notifier(SEND_RETRY_ATTEMPTS=3, SEND_RETRY_BACKOFF_SECONDS=0)
    attempts = {"count": 0}

    def fake_post(*args, **kwargs):
        attempts["count"] += 1

        class Response:
            @staticmethod
            def raise_for_status():
                return None

            @staticmethod
            def json():
                return {
                    "error": {
                        "error_code": 917,
                        "error_msg": "You don't have access to this chat",
                    }
                }

        return Response()

    monkeypatch.setattr(requests, "post", fake_post)

    success, result = notifier._internal_send_vk_message("message")

    assert success is False
    assert result["error_code"] == 917
    assert attempts["count"] == 1
