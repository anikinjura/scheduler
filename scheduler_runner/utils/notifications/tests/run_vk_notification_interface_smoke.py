#!/usr/bin/env python3
"""
Production-style E2E smoke для проверки VK через общий notifications interface.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from config.base_config import ENV_MODE, PVZ_ID
from scheduler_runner.utils.notifications import send_notification, test_connection


def _mode_suffix() -> str:
    return "PROD" if ENV_MODE == "production" else "TEST"


def build_connection_params() -> dict:
    suffix = _mode_suffix()
    return {
        "NOTIFICATION_PROVIDER": "vk",
        "VK_ACCESS_TOKEN": os.environ.get(f"VK_ACCESS_TOKEN_{suffix}"),
        "VK_PEER_ID": os.environ.get(f"VK_PEER_ID_{suffix}"),
        "VK_API_VERSION": os.environ.get("VK_API_VERSION", "5.199"),
    }


def build_default_message(label: str | None = None) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{label}] " if label else ""
    return (
        f"{prefix}VK notification interface smoke\n"
        f"ENV_MODE: {ENV_MODE}\n"
        f"PVZ_ID: {PVZ_ID}\n"
        f"Timestamp: {timestamp}\n"
        f"Status: synthetic opening event\n"
        f"Started at: 08:45:00 (источник: smoke)"
    )


def main():
    parser = argparse.ArgumentParser(description="Production-style VK smoke через notifications interface")
    parser.add_argument("--message", help="Явный текст сообщения")
    parser.add_argument("--label", help="Короткая метка для префикса сообщения")
    parser.add_argument(
        "--mode",
        choices=["send", "check", "both"],
        default="send",
        help="send: реальная отправка, check: только diagnostic check, both: check + send",
    )
    parser.add_argument("--pretty", action="store_true", help="Печатать результат в человекочитаемом виде")
    args = parser.parse_args()

    connection_params = build_connection_params()
    token = connection_params.get("VK_ACCESS_TOKEN")
    peer_id = connection_params.get("VK_PEER_ID")

    result = {
        "success": True,
        "mode": args.mode,
        "env_mode": ENV_MODE,
        "pvz_id": PVZ_ID,
        "has_token": bool(token),
        "has_peer_id": bool(peer_id),
        "api_version": connection_params.get("VK_API_VERSION"),
        "peer_id": str(peer_id) if peer_id else None,
    }

    if args.mode in {"check", "both"}:
        result["connection_check"] = test_connection(connection_params=connection_params)
        result["success"] = result["success"] and result["connection_check"].get("success", False)

    if args.mode in {"send", "both"}:
        message = args.message or build_default_message(args.label)
        result["message_preview"] = message[:160]
        result["send_result"] = send_notification(message=message, connection_params=connection_params)
        result["success"] = result["success"] and result["send_result"].get("success", False)

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
