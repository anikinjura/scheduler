#!/usr/bin/env python3
"""
E2E smoke для проверки VK notification-уведомлений.

Использует ТОТ ЖЕ путь, что и production-код:
- send_notification_microservice из reports_notifications.py
- connection_params из reports_paths.py → REPORTS_PATHS['NOTIFICATION_CONNECTION_PARAMS']

Этот тест должен падать/проходить в зависимости от:
- валидности VK_ACCESS_TOKEN_TEST в .env\\secrets.env
- доступности VK Peer ID (VK_PEER_ID_TEST)
- доступности VK API

Если production-код меняет способ отправки (другой модуль, другие параметры),
этот тест должен упасть — это сигнализирует о необходимости обновления смоки.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Добавляем корень проекта в sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.base_config import ENV_MODE, PVZ_ID

# Production-модули — тот же путь что и reports_processor.py
from ..reports_notifications import (
    create_notification_logger,
    send_notification_microservice,
)
from ..config.reports_paths import REPORTS_PATHS


def build_default_message(label: str | None = None) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{label}] " if label else ""
    return (
        f"{prefix}VK notification e2e smoke\n"
        f"ENV_MODE: {ENV_MODE}\n"
        f"PVZ_ID: {PVZ_ID}\n"
        f"Timestamp: {timestamp}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="VK E2E smoke через production send_notification_microservice"
    )
    parser.add_argument("--message", help="Явный текст сообщения")
    parser.add_argument("--label", help="Короткая метка для префикса сообщения")
    parser.add_argument("--pretty", action="store_true", help="Печатать результат в человекочитаемом виде")
    args = parser.parse_args()

    logger = create_notification_logger()

    # Connection params из того же источника, что и production-код
    connection_params = REPORTS_PATHS.get("NOTIFICATION_CONNECTION_PARAMS", {})
    provider = connection_params.get("NOTIFICATION_PROVIDER", "telegram")
    token = connection_params.get("VK_ACCESS_TOKEN") or connection_params.get("TELEGRAM_BOT_TOKEN")
    target = (
        connection_params.get("VK_PEER_ID")
        or connection_params.get("TELEGRAM_CHAT_ID")
    )

    result = {
        "success": True,
        "env_mode": ENV_MODE,
        "pvz_id": PVZ_ID,
        "provider": provider,
        "has_token": bool(token),
        "has_target": bool(target),
        "token_length": len(token) if token else 0,
        "target_preview": str(target)[:20] if target else None,
    }

    if not connection_params:
        result["success"] = False
        result["error"] = "Отсутствуют параметры подключения в REPORTS_PATHS"
    elif provider != "vk":
        result["success"] = False
        result["error"] = (
            f"Provider '{provider}' не VK. "
            f"Установите NOTIFICATION_PROVIDER_{ENV_MODE.upper()}=vk в окружении."
        )
    else:
        message = args.message or build_default_message(args.label)
        result["message_preview"] = message[:120]
        result["send_result"] = send_notification_microservice(message, logger=logger)
        result["success"] = result["success"] and result["send_result"].get("success", False)

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))

    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
