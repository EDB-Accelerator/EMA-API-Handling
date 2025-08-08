#!/usr/bin/env python3
"""
set_interactions_from_json – upload m-Path interactions from a local JSON structure or file.

Public API (importable):
    from ema_api_handling.set_interactions_from_json import set_interactions, upload_interactions_file

Environment (used if not overridden interactively):
    MPATH_USERCODE        – required (5-char code)
    MPATH_PRIVKEY         – optional (defaults to ~/.mpath_private_key.pem)
    MPATH_CONNECTION_ID   – optional; if absent and running in a TTY, you will be prompted

Notes:
    • Payload is sent via querystring parameter "interactionsJSON", as required by the API.
    • Base URL defaults to https://dashboard.m-path.io/API2; override with MPATH_BASE_URL if needed.
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

__all__ = ["set_interactions", "upload_interactions_file"]

# You can switch to "https://m-path.io/API2" if your tenant uses that domain.
BASE_URL = os.getenv("MPATH_BASE_URL", "https://dashboard.m-path.io/API2")


# ─────────────────────────────────────────────── helpers

def _require_user_code() -> str:
    uc = os.getenv("MPATH_USERCODE")
    if not uc:
        sys.exit("MPATH_USERCODE is not set (see generate_keys.py).")
    return uc


def _resolve_connection_id() -> int:
    env_val = os.getenv("MPATH_CONNECTION_ID", "")
    if env_val.isdigit():
        return int(env_val)

    # Prompt only if interactive; otherwise fail fast with a clear message
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


# ─────────────────────────────────────────────── public API

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

    Parameters
    ----------
    interactions : list[dict]
        The interactions JSON (already parsed) to upload.
    user_code : str, optional
        5-character user code; if None, reads MPATH_USERCODE.
    connection_id : int, optional
        Connection ID; if None, reads MPATH_CONNECTION_ID or prompts on TTY.
    private_key_pem : Path | str, optional
        Path to RSA private key; if None, uses MPATH_PRIVKEY or ~/.mpath_private_key.pem.
    base_url : str, optional
        API base URL; defaults to env MPATH_BASE_URL or https://dashboard.m-path.io/API2.
    timeout : int
        Requests timeout in seconds.

    Returns
    -------
    dict
        Parsed JSON reply from the server.

    Raises
    ------
    RuntimeError
        If the API returns a non-success status.
    requests.HTTPError
        For non-2xx HTTP responses.
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

    try:
        body = resp.json()
    except ValueError:
        # Server sent non-JSON; surface raw text for debugging
        print("Raw response (non-JSON):\n", resp.text)
        raise

    # Pretty print for convenience when called from notebooks/CLIs
    print(json.dumps(body, indent=2, ensure_ascii=False))

    if body.get("status") != 1:
        raise RuntimeError("API rejected the payload.")

    return body


def upload_interactions_file(
    path: Path | str,
    **kwargs,
) -> Dict:
    """
    Convenience wrapper: load interactions from a JSON file and call set_interactions().

    Parameters
    ----------
    path : str | Path
        Path to a JSON file containing the interactions list.
    **kwargs :
        Forwarded to set_interactions (e.g., user_code, connection_id, private_key_pem, base_url, timeout).

    Returns
    -------
    dict
        Parsed JSON reply from the server.
    """
    p = Path(path).expanduser()
    if not p.is_file():
        raise FileNotFoundError(f"{p} does not exist.")

    interactions = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(interactions, list):
        raise ValueError("Expected the JSON file to contain a list of interaction blocks.")
    return set_interactions(interactions, **kwargs)


# ─────────────────────────────────────────────── CLI

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

    upload_interactions_file(
        args.json_file,
        user_code=args.user_code,
        connection_id=args.connection_id,
        private_key_pem=args.privkey,
        base_url=args.base_url,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    _cli()
