#!/usr/bin/env python3
"""
set_interactions_from_json – upload m-Path interactions from a local JSON structure.

Usage (package API):
    from ema_api_handling.set_interactions_from_json import set_interactions
    set_interactions(interactions)

Environment:
    MPATH_USERCODE     – required (5-char code)
    MPATH_PRIVKEY      – optional (defaults to ~/.mpath_private_key.pem)
    MPATH_CONNECTION_ID– optional; if absent, function will prompt
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict

import jwt
import requests

__all__ = ["set_interactions"]

BASE_URL = "https://m-path.io/API2"

def _get_user_code() -> str:
    uc = os.getenv("MPATH_USERCODE")
    if not uc:
        sys.exit("MPATH_USERCODE is not set (see generate_keys.py).")
    return uc

def _get_connection_id() -> int:
    env_val = os.getenv("MPATH_CONNECTION_ID", "")
    if env_val.isdigit():
        return int(env_val)
    # prompt only if interactive
    if sys.stdin.isatty():
        return int(input("Enter numeric CONNECTION ID: ").strip())
    raise RuntimeError("MPATH_CONNECTION_ID not set and no TTY to prompt.")

def _get_privkey_path() -> Path:
    p = Path(os.getenv("MPATH_PRIVKEY", Path.home() / ".mpath_private_key.pem"))
    if not p.exists():
        sys.exit(f"RSA private key not found: {p}")
    return p

def _make_jwt(user_code: str, key_path: Path, ttl_minutes: int = 5) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    payload = {"exp": exp, "userCode": user_code}
    return jwt.encode(payload, key_path.read_text(), algorithm="RS256")

def set_interactions(interactions: List[Dict]) -> None:
    """
    Upload a replacement interaction (questionnaire) list to m-Path.
    Sends the full JSON as a query string parameter (not as body).
    """
    user_code = _get_user_code()
    connection_id = _get_connection_id()
    key_path = _get_privkey_path()

    payload = json.dumps(interactions, ensure_ascii=False)
    params = {
        "userCode": user_code,
        "connectionId": connection_id,
        "JWT": _make_jwt(user_code, key_path),
        "interactionsJSON": payload,
    }

    resp = requests.post(f"{BASE_URL}/setInteractions", params=params, timeout=30)
    resp.raise_for_status()

    try:
        body = resp.json()
    except ValueError:
        print("Raw response (non-JSON):\n", resp.text)
        return

    print("Server reply:\n", json.dumps(body, indent=2, ensure_ascii=False))

    if body.get("status") != 1:
        raise RuntimeError("API rejected the payload.")
