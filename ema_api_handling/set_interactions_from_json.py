#!/usr/bin/env python3
"""
set_interactions_from_json – upload m-Path interactions from a local JSON structure or file.

Public API:
    from ema_api_handling import set_interactions_from_json
    set_interactions_from_json(interactions)

Also available:
    from ema_api_handling import set_interactions
    set_interactions(interactions)

Environment (used if not provided explicitly):
    MPATH_USERCODE        – required (5-char code)
    MPATH_PRIVKEY         – optional (defaults to ~/.mpath_private_key.pem)
    MPATH_CONNECTION_ID   – optional; if absent and running in a TTY, you will be prompted
    MPATH_BASE_URL        – optional; defaults to https://dashboard.m-path.io/API2
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Optional

import jwt
import requests

__all__ = ["set_interactions", "set_interactions_from_json"]

# Default base; override with env MPATH_BASE_URL if needed
BASE_URL = os.getenv("MPATH_BASE_URL", "https://dashboard.m-path.io/API2")


# ───────────────────────── helpers

def _require_user_code() -> str:
    uc = os.getenv("MPATH_USERCODE")
    if not uc:
        sys.exit("MPATH_USERCODE is not set (see generate_keys.py).")
    return uc

def _resolve_connection_id() -> int:
    env_val = os.getenv("MPATH_CONNECTION_ID", "")
    if env_val.isdigit():
        return int(env_val)
    if sys.stdin.isatty():
        val = input("Enter numeric CONNECTION ID: ").strip()
        if not val.isdigit():
            raise ValueError("CONNECTION ID must be an integer.")
        return int(val)
    raise RuntimeError("MPATH_CONNECTION_ID not set and no TTY available to prompt.")

def _resolve_privkey_path() -> Path:
    p = Path(os.getenv("MPATH_PRIVKEY", Path.home() / ".mpath_private_key.pem")).expanduser()
    if not p.exists():
        sys.exit(f"RSA private key not found: {p}")
    return p

def _make_jwt(user_code: str, key_path: Path, ttl_minutes: int = 5) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    payload = {"exp": int(exp.timestamp()), "userCode": user_code}
    return jwt.encode(payload, key_path.read_text(), algorithm="RS256")


# ───────────────────────── public API

def set_interactions(
    interactions: List[Dict],
    *,
    user_code: Optional[str] = None,
    connection_id: Optional[int] = None,
    private_key_pem: Optional[Path | str] = None,
    base_url: Optional[str] = None,
    timeout: int = 30,
) -> Dict:
    """
    Upload a replacement list of interactions (questionnaires) to m-Path.
    """
    user_code = user_code or _require_user_code()
    connection_id = connection_id if connection_id is not None else _resolve_connection_id()
    key_path = Path(private_key_pem).expanduser() if private_key_pem else _resolve_privkey_path()
    api_base = base_url or BASE_URL

    payload = json.dumps(interactions, ensure_ascii=False)
    params = {
        "userCode": user_code,
        "connectionId": connection_id,
        "JWT": _make_jwt(user_code, key_path),
        "interactionsJSON": payload,
    }

    resp = requests.post(f"{api_base}/setInteractions", params=params, timeout=timeout)
    resp.raise_for_status()

    body = resp.json()  # raise if non-JSON
    print(json.dumps(body, indent=2, ensure_ascii=False))
    if body.get("status") != 1:
        raise RuntimeError("API rejected the payload.")
    return body


def set_interactions_from_json(
    interactions: List[Dict],
    **kwargs,
) -> Dict:
    """
    Alias for set_interactions(interactions, **kwargs), provided for the exact name you want to import.
    """
    return set_interactions(interactions, **kwargs)


# ───────────────────────── optional CLI

def _cli() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Upload m-Path interactions from a local JSON file.")
    ap.add_argument("json_file", help="Path to interactions JSON file.")
    ap.add_argument("--user-code", help="5-char m-Path user code (overrides env).")
    ap.add_argument("--connection-id", type=int, help="m-Path connection ID (overrides env/prompt).")
    ap.add_argument("--privkey", type=str, help="Path to RSA private key PEM.")
    ap.add_argument("--base-url", type=str, help="API base URL (default: env MPATH_BASE_URL or dashboard.m-path.io/API2).")
    ap.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds (default: 30).")
    args = ap.parse_args()

    p = Path(args.json_file).expanduser()
    if not p.is_file():
        raise FileNotFoundError(f"{p} does not exist.")
    interactions = json.loads(p.read_text(encoding="utf-8"))

    set_interactions_from_json(
        interactions,
        user_code=args.user_code,
        connection_id=args.connection_id,
        private_key_pem=args.privkey,
        base_url=args.base_url,
        timeout=args.timeout,
    )

if __name__ == "__main__":
    _cli()
