#!/usr/bin/env python3
"""
E2E smoke для проверки notification-уведомлений через refactored модули.

Импортирует из refactored_modules.reports_notifications вместо боевого reports_processor.py.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Добавляем корень проекта в sys.path для импортов config и utils
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.base_config import ENV_MODE, PVZ_ID
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS
from scheduler_runner.tasks.reports.reports_notifications import (
    create_notification_logger,
    send_notification_microservice,
)
from scheduler_runner.utils.notifications import test_connection as test_notification_connection


def build_connection_params() -> dict:
    return dict(REPORTS_PATHS.get("NOTIFICATION_CONNECTION_PARAMS", {}))


def build_default_message(label: str | None = None) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{label}] " if label else ""
    return (
        f"{prefix}Refactored reports notification e2e smoke\n"
        f"ENV_MODE: {ENV_MODE}\n"
        f"PVZ_ID: {PVZ_ID}\n"
        f"Timestamp: {timestamp}"
    )


def main():
    parser = argparse.ArgumentParser(description="E2E smoke для проверки notification send-path из refactored modules")
    parser.add_argument("--message", help="Явный текст сообщения")
    parser.add_argument("--label", help="Короткая метка для префикса сообщения")
    parser.add_argument(
        "--mode",
        choices=["send", "check", "both"],
        default="send",
        help="send: реальная отправка, check: только diagnostic getMe, both: сначала check, затем send",
    )
    parser.add_argument("--pretty", action="store_true", help="Печатать результат в человекочитаемом виде")
    args = parser.parse_args()

    logger = create_notification_logger()
    connection_params = build_connection_params()
    provider = connection_params.get("NOTIFICATION_PROVIDER", "telegram")
    token = connection_params.get("TELEGRAM_BOT_TOKEN") or connection_params.get("VK_ACCESS_TOKEN")
    chat_id = connection_params.get("TELEGRAM_CHAT_ID") or connection_params.get("VK_PEER_ID")

    result = {
        "success": True,
        "mode": args.mode,
        "env_mode": ENV_MODE,
        "pvz_id": PVZ_ID,
        "provider": provider,
        "has_token": bool(token),
        "has_chat_id": bool(chat_id),
        "token_length": len(token) if token else 0,
        "chat_id_preview": str(chat_id)[-6:] if chat_id else None,
    }

    if args.mode in {"check", "both"}:
        result["connection_check"] = test_notification_connection(connection_params, logger=logger)
        result["success"] = result["success"] and result["connection_check"].get("success", False)

    if args.mode in {"send", "both"}:
        message = args.message or build_default_message(args.label)
        result["message_preview"] = message[:120]
        result["send_result"] = send_notification_microservice(message, logger=logger)
        result["success"] = result["success"] and result["send_result"].get("success", False)

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

