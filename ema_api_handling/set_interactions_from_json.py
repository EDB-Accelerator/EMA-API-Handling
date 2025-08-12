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
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Optional

import jwt
import requests
from requests import RequestException, Timeout, ConnectionError as ReqConnectionError

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


# ───────────────────────── low-level POST helpers (avoid 414)

def _try_post_variants(url: str, *, form: dict, payload_min: str, timeout: int) -> requests.Response:
    """
    Try several encodings so the backend accepts large interactions safely:
      1) application/x-www-form-urlencoded  (data=)
      2) application/json                   (json=)
      3) multipart/form-data                (files= for interactionsJSON)
    Returns the first response received (even if non-2xx); caller raises if needed.
    """
    # 1) x-www-form-urlencoded
    try:
        resp = requests.post(url, data=form, timeout=timeout)
        if resp.status_code != 400 and resp.status_code != 415:
            return resp
    except requests.RequestException:
        pass

    # 2) JSON body
    try:
        resp = requests.post(url, json=form, timeout=timeout)
        if resp.status_code != 400 and resp.status_code != 415:
            return resp
    except requests.RequestException:
        pass

    # 3) multipart/form-data — send large JSON as a file-like field
    try:
        data = {k: v for k, v in form.items() if k != "interactionsJSON"}
        files = {
            # filename is optional; some backends parse better when provided
            "interactionsJSON": ("interactions.json", payload_min, "application/json"),
        }
        resp = requests.post(url, data=data, files=files, timeout=timeout)
        return resp
    except requests.RequestException as e:
        # Bubble up; outer retry loop will handle
        raise RuntimeError(f"Network error when posting setInteractions: {e}") from e


# ───────────────────────── core uploader with retries

def _post_set_interactions(
    *,
    user_code: str,
    connection_id: int,
    key_path: Path,
    interactions: List[Dict],
    base_url: str,
    timeout: int,
) -> Dict:
    """
    Single logical POST call; returns parsed JSON (or raises).
    Sends interactions in the request body (not the URL) to avoid 414 URI Too Long.
    """
    # Minify JSON to reduce payload size
    payload_min = json.dumps(interactions, ensure_ascii=False, separators=(",", ":"))

    form = {
        "userCode": user_code,
        "connectionId": str(connection_id),
        "JWT": _make_jwt(user_code, key_path),
        "interactionsJSON": payload_min,
    }

    url = f"{base_url}/setInteractions"
    resp = _try_post_variants(url, form=form, payload_min=payload_min, timeout=timeout)
    resp.raise_for_status()

    try:
        return resp.json()
    except ValueError:
        # Surface raw text for debugging if server didn't return JSON
        raise RuntimeError(f"Non-JSON response: {resp.text[:1000]}...")


# ───────────────────────── public API

def set_interactions(
    interactions: List[Dict],
    *,
    user_code: Optional[str] = None,
    connection_id: Optional[int] = None,
    private_key_pem: Optional[Path | str] = None,
    base_url: Optional[str] = None,
    timeout: int = 30,
    retries: int = 3,
    backoff_seconds: int = 5,
    verbose: bool = True,
) -> Dict:
    """
    Upload a replacement list of interactions (questionnaires) to m-Path.

    Retries on:
      • API body {"status": -1}
      • network timeouts / connection errors
      • other RequestException / HTTPError (e.g., 429/5xx)

    Parameters
    ----------
    interactions : list[dict]
    user_code : str | None
    connection_id : int | None
    private_key_pem : Path | str | None
    base_url : str | None
    timeout : int
    retries : int
        Total attempts = retries (minimum 1).
    backoff_seconds : int
        Sleep time between retry attempts.
    verbose : bool
        Print server reply and retry messages.

    Returns
    -------
    dict
        Parsed JSON reply (status==1) on success.

    Raises
    ------
    RuntimeError on non-success status or after exhausting retries.
    requests.HTTPError for non-2xx responses that are not resolved by retries.
    """
    if retries < 1:
        retries = 1

    user_code = user_code or _require_user_code()
    connection_id = connection_id if connection_id is not None else _resolve_connection_id()
    key_path = Path(private_key_pem).expanduser() if private_key_pem else _resolve_privkey_path()
    api_base = base_url or BASE_URL

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            body = _post_set_interactions(
                user_code=user_code,
                connection_id=connection_id,
                key_path=key_path,
                interactions=interactions,
                base_url=api_base,
                timeout=timeout,
            )
            if verbose:
                print(json.dumps(body, indent=2, ensure_ascii=False))

            status = body.get("status")
            if status == 1:
                return body

            # Transient server state: retry on status -1
            if status == -1 and attempt < retries:
                if verbose:
                    print(f"status -1; retrying … [{attempt}/{retries}]")
                time.sleep(backoff_seconds)
                continue

            raise RuntimeError(f"API rejected the payload: {json.dumps(body, ensure_ascii=False)}")

        except (Timeout, ReqConnectionError, RequestException) as e:
            # HTTPError is a RequestException; allow retry on common transient cases
            last_err = e
            if attempt < retries:
                if verbose:
                    print(f"{e.__class__.__name__}: {e}. Retrying … [{attempt}/{retries}]")
                time.sleep(backoff_seconds)
                continue
            raise  # bubble up the last requests exception

    # If we somehow exit loop without return/raise (shouldn't happen)
    if last_err:
        raise last_err
    raise RuntimeError("Failed to upload interactions (unknown error).")


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
    ap.add_argument("--retries", type=int, default=3, help="Number of attempts for transient failures (default: 3).")
    ap.add_argument("--backoff", type=int, default=5, help="Seconds to wait between retries (default: 5).")
    ap.add_argument("--no-verbose", action="store_true", help="Suppress printing server replies and retry messages.")
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
        retries=args.retries,
        backoff_seconds=args.backoff,
        verbose=not args.no_verbose,
    )

if __name__ == "__main__":
    _cli()
