#!/usr/bin/env python3
"""
Временный E2E smoke для проверки VK notifications.

Скрипт использует общий config/bootstrap:
- в первую очередь явные CLI overrides;
- затем process env / `.env\\secrets.env` через `config.base_config`;
- для test/prod выбирает `VK_ACCESS_TOKEN_<MODE>` и `VK_PEER_ID_<MODE>`.

Назначение:
- discover: получить список доступных бесед и их peer_id
- send: отправить synthetic сообщение в указанный peer_id
- both: сначала discover, затем send
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from config.base_config import ENV_MODE, PVZ_ID


VK_API_URL = "https://api.vk.com/method"
DEFAULT_API_VERSION = "5.199"


def _mode_suffix() -> str:
    return "PROD" if ENV_MODE == "production" else "TEST"


def build_vk_runtime_config() -> dict[str, str | None]:
    suffix = _mode_suffix()
    return {
        "token": os.environ.get(f"VK_ACCESS_TOKEN_{suffix}"),
        "peer_id": os.environ.get(f"VK_PEER_ID_{suffix}"),
        "api_version": os.environ.get("VK_API_VERSION") or DEFAULT_API_VERSION,
    }


def mask_token(token: str | None) -> str | None:
    if not token:
        return None
    if len(token) <= 10:
        return "*" * len(token)
    return f"{token[:6]}...{token[-6:]}"


def vk_call(method: str, token: str, api_version: str, **params: Any) -> dict[str, Any]:
    url = f"{VK_API_URL}/{method}"
    payload = {
        **params,
        "access_token": token,
        "v": api_version,
    }
    response = requests.post(url, data=payload, timeout=30)
    response.raise_for_status()
    body = response.json()
    return body


def discover_conversations(token: str, api_version: str, count: int = 200) -> dict[str, Any]:
    body = vk_call("messages.getConversations", token, api_version, count=count)
    if "error" in body:
        return {
            "success": False,
            "error": body["error"],
            "conversations": [],
        }

    items = body.get("response", {}).get("items", [])
    conversations: list[dict[str, Any]] = []
    for item in items:
        conversation = item.get("conversation", {})
        peer = conversation.get("peer", {})
        chat_settings = conversation.get("chat_settings", {}) or {}
        local_id = chat_settings.get("local_id")
        peer_id = peer.get("id")
        peer_type = peer.get("type")

        conversations.append(
            {
                "peer_id": peer_id,
                "peer_type": peer_type,
                "chat_id": local_id,
                "title": chat_settings.get("title"),
                "members_count": chat_settings.get("members_count"),
                "state": conversation.get("state"),
                "can_write": conversation.get("can_write", {}).get("allowed"),
            }
        )

    return {
        "success": True,
        "count": len(conversations),
        "conversations": conversations,
    }


def build_default_message(label: str | None = None) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{label}] " if label else ""
    return (
        f"{prefix}VK notification e2e smoke\n"
        f"ENV_MODE: {ENV_MODE}\n"
        f"PVZ_ID: {PVZ_ID}\n"
        f"Timestamp: {timestamp}\n"
        f"Status: synthetic opening event\n"
        f"Started at: 08:45:00 (источник: smoke)"
    )


def resolve_peer_id(args_peer_id: int | None, args_chat_id: int | None, discover_result: dict[str, Any] | None) -> tuple[int | None, str | None]:
    if args_peer_id:
        return args_peer_id, "explicit peer_id"

    if args_chat_id is not None:
        return 2_000_000_000 + args_chat_id, "chat_id -> peer_id"

    if discover_result and discover_result.get("success"):
        chats = [c for c in discover_result.get("conversations", []) if c.get("peer_type") == "chat"]
        if len(chats) == 1 and chats[0].get("peer_id"):
            return chats[0]["peer_id"], "auto-discovered single chat"

    return None, None


def send_message(token: str, api_version: str, peer_id: int, message: str) -> dict[str, Any]:
    body = vk_call(
        "messages.send",
        token,
        api_version,
        peer_id=peer_id,
        random_id=int(time.time() * 1000) + random.randint(0, 999),
        message=message,
    )
    if "error" in body:
        return {
            "success": False,
            "error": body["error"],
        }

    return {
        "success": True,
        "response": body.get("response"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Временный E2E smoke для VK notifications")
    parser.add_argument(
        "--mode",
        choices=["discover", "send", "both"],
        default="discover",
        help="discover: получить peer_id бесед, send: отправка сообщения, both: discover + send",
    )
    parser.add_argument("--token", help="VK community token; explicit override over bootstrap/runtime config")
    parser.add_argument("--api-version", help="VK API version; explicit override over bootstrap/runtime config")
    parser.add_argument("--peer-id", type=int, help="Явный peer_id назначения")
    parser.add_argument("--chat-id", type=int, help="Явный chat_id; будет преобразован в peer_id")
    parser.add_argument("--message", help="Явный текст сообщения")
    parser.add_argument("--label", help="Короткая метка для префикса сообщения")
    parser.add_argument("--count", type=int, default=200, help="Сколько бесед читать в discover")
    parser.add_argument("--pretty", action="store_true", help="Печатать результат в человекочитаемом виде")
    args = parser.parse_args()

    runtime_config = build_vk_runtime_config()
    token = args.token or runtime_config["token"] or os.environ.get("VK_SMOKE_TOKEN")
    api_version = args.api_version or runtime_config["api_version"] or DEFAULT_API_VERSION
    result: dict[str, Any] = {
        "success": True,
        "mode": args.mode,
        "env_mode": ENV_MODE,
        "pvz_id": PVZ_ID,
        "has_token": bool(token),
        "token_preview": mask_token(token),
        "api_version": api_version,
        "bootstrap_peer_id": runtime_config["peer_id"],
    }

    if not token:
        result["success"] = False
        result["error"] = "VK community token не передан"
    else:
        discover_result: dict[str, Any] | None = None

        if args.mode in {"discover", "both"}:
            discover_result = discover_conversations(token, api_version, count=args.count)
            result["discover_result"] = discover_result
            result["success"] = result["success"] and discover_result.get("success", False)

        if args.mode in {"send", "both"}:
            bootstrap_peer_id = int(runtime_config["peer_id"]) if runtime_config["peer_id"] else None
            explicit_or_bootstrap_peer = args.peer_id if args.peer_id is not None else bootstrap_peer_id
            peer_id, peer_source = resolve_peer_id(explicit_or_bootstrap_peer, args.chat_id, discover_result)
            result["resolved_peer_id"] = peer_id
            result["peer_resolution"] = peer_source

            if not peer_id:
                result["success"] = False
                result["send_result"] = {
                    "success": False,
                    "error": "Не удалось определить peer_id. Передайте --peer-id/--chat-id или используйте discover с единственной доступной беседой.",
                }
            else:
                message = args.message or build_default_message(args.label)
                result["message_preview"] = message[:160]
                send_result = send_message(token, api_version, peer_id, message)
                result["send_result"] = send_result
                result["success"] = result["success"] and send_result.get("success", False)

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
