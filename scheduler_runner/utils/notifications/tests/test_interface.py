from scheduler_runner.utils.notifications import interface


class DummyNotifier:
    def __init__(self, config=None, logger=None):
        self.config = config or {}
        self.logger = logger

    def connect(self):
        return True

    def disconnect(self):
        return True

    def send_notification(self, message, templates=None):
        return {"success": True, "provider": self.config.get("NOTIFICATION_PROVIDER", "telegram"), "message": message}

    def send_batch_notifications(self, messages, templates=None):
        return {"success": True, "provider": self.config.get("NOTIFICATION_PROVIDER", "telegram"), "sent": len(messages)}

    def _validate_connection_params(self):
        return True

    def _establish_connection(self):
        return True


def test_send_notification_uses_telegram_by_default(monkeypatch):
    monkeypatch.setattr(interface, "TelegramNotifier", DummyNotifier)
    monkeypatch.setattr(interface, "VkNotifier", DummyNotifier)

    result = interface.send_notification(
        message="hello",
        connection_params={"TELEGRAM_BOT_TOKEN": "token", "TELEGRAM_CHAT_ID": "chat"},
    )

    assert result["success"] is True
    assert result["provider"] == "telegram"


def test_send_notification_can_use_vk_provider(monkeypatch):
    monkeypatch.setattr(interface, "TelegramNotifier", DummyNotifier)
    monkeypatch.setattr(interface, "VkNotifier", DummyNotifier)

    result = interface.send_notification(
        message="hello",
        connection_params={"VK_ACCESS_TOKEN": "token", "VK_PEER_ID": "2000000001", "NOTIFICATION_PROVIDER": "vk"},
    )

    assert result["success"] is True
    assert result["provider"] == "vk"


def test_test_connection_uses_vk_provider(monkeypatch):
    monkeypatch.setattr(interface, "TelegramNotifier", DummyNotifier)
    monkeypatch.setattr(interface, "VkNotifier", DummyNotifier)

    result = interface.test_connection(
        connection_params={"VK_ACCESS_TOKEN": "token", "VK_PEER_ID": "2000000001", "NOTIFICATION_PROVIDER": "vk"},
    )

    assert result["success"] is True
    assert result["connection_params_valid"] is True
