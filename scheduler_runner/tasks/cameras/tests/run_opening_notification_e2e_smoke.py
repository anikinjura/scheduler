#!/usr/bin/env python3
"""
E2E smoke для проверки Telegram-уведомлений OpeningMonitorScript.

Скрипт использует те же Telegram credentials, что и production flow
`OpeningMonitorScript.send_telegram_notification()`.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from config.base_config import ENV_MODE, PVZ_ID
from scheduler_runner.tasks.cameras.OpeningMonitorScript import (
    SCRIPT_CONFIG,
    create_notification_logger,
    send_telegram_notification,
)
from scheduler_runner.utils.notifications import test_connection as test_notification_connection


def build_connection_params() -> dict:
    token = SCRIPT_CONFIG.get("TELEGRAM_TOKEN")
    chat_id = SCRIPT_CONFIG.get("TELEGRAM_CHAT_ID")
    return {
        "TELEGRAM_BOT_TOKEN": token,
        "TELEGRAM_CHAT_ID": chat_id,
    }


def build_default_message(label: str | None = None) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{label}] " if label else ""
    return (
        f"{prefix}OpeningMonitor notification e2e smoke\n"
        f"ENV_MODE: {ENV_MODE}\n"
        f"PVZ_ID: {PVZ_ID}\n"
        f"Timestamp: {timestamp}\n"
        f"Status: synthetic opening event\n"
        f"Started at: 08:45:00 (источник: smoke)"
    )


def main():
    parser = argparse.ArgumentParser(description="E2E smoke для проверки Telegram send-path OpeningMonitorScript")
    parser.add_argument("--message", help="Явный текст сообщения")
    parser.add_argument("--label", help="Короткая метка для префикса сообщения")
    parser.add_argument(
        "--mode",
        choices=["send", "check", "both"],
        default="send",
        help="send: реальная отправка current opening flow, check: только diagnostic getMe, both: сначала check, затем send",
    )
    parser.add_argument("--pretty", action="store_true", help="Печатать результат в человекочитаемом виде")
    args = parser.parse_args()

    logger = create_notification_logger()
    connection_params = build_connection_params()
    token = connection_params.get("TELEGRAM_BOT_TOKEN")
    chat_id = connection_params.get("TELEGRAM_CHAT_ID")

    result = {
        "success": True,
        "mode": args.mode,
        "env_mode": ENV_MODE,
        "pvz_id": PVZ_ID,
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
        send_success = send_telegram_notification(message=message)
        result["send_result"] = {"success": send_success}
        result["success"] = result["success"] and send_success

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
