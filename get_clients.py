#!/usr/bin/env python3
"""
get_clients.py – download raw client metadata from m-Path and save JSON files (no flattening)
Author: Kyunghun Lee (kyunghun.lee@nih.gov)
Updated: 2025-07-24

MIT License
Copyright (c) 2025 Kyunghun Lee
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Tuple, List, Dict, Optional

import jwt
import requests

# ─────────────────────────────────────────────── 0 | SETTINGS
PRIVATE_KEY_PEM: Path = Path.home() / ".mpath_private_key.pem"
PUBLIC_KEY_PEM: Path  = Path.home() / ".mpath_public_key.pem"  # kept for parity
BASE_DUMP_DIR: Path   = Path("mpath_clients").expanduser()
BASE_URL: str         = "https://dashboard.m-path.io/API2"
# BASE_URL: str       = "https://m-path.io/API2"  # legacy

DEFAULT_CHANGED_AFTER_UTC = "2024-01-01 00:00:00"  # Safe default per Stijn’s note

# ─────────────────────────────────────────────── 1 | JWT

def make_jwt(user_code: str, ttl_minutes: int = 5) -> str:
    """Generate a signed JWT for m-Path authentication."""
    private_key = PRIVATE_KEY_PEM.read_text()
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    payload = {"exp": int(exp.timestamp()), "userCode": user_code}
    return jwt.encode(payload, private_key, algorithm="RS256")

# ─────────────────────────────────────────────── 2 | HELPERS

def normalize_changed_after(dt_str: Optional[str]) -> Optional[str]:
    """
    Normalize a user-supplied datetime string to 'YYYY-MM-DD HH:MM:SS' (UTC).
    - If dt_str is None, return the DEFAULT_CHANGED_AFTER_UTC.
    - Accepts:
        * 'YYYY-MM-DD' → treated as 'YYYY-MM-DD 00:00:00'
        * 'YYYY-MM-DD HH:MM:SS'
    Raises ValueError on invalid format.
    """
    if dt_str is None:
        return DEFAULT_CHANGED_AFTER_UTC

    dt_str = dt_str.strip()
    if not dt_str:
        return DEFAULT_CHANGED_AFTER_UTC

    # Try date-only format
    try:
        d = datetime.strptime(dt_str, "%Y-%m-%d")
        return d.strftime("%Y-%m-%d 00:00:00")
    except ValueError:
        pass

    # Try full datetime format
    try:
        d = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return d.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise ValueError(
            f"changedAfterUTC must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'. Got: {dt_str}"
        )

def _sanitize_url(url: str) -> str:
    """Hide JWT token when printing URLs."""
    # naive masking: replace 'JWT=' value with '<redacted>'
    if "JWT=" in url:
        parts = url.split("JWT=")
        head = parts[0]
        tail = parts[1]
        # Tail may have '&'; keep structure but replace token
        rest = tail.split("&", 1)
        if len(rest) == 2:
            token, rest_params = rest
            return f"{head}JWT=<redacted>&{rest_params}"
        else:
            return f"{head}JWT=<redacted>"
    return url

# ─────────────────────────────────────────────── 3 | API CORE

def _call_raw(endpoint: str, *, show_url: bool = False, **params) -> Dict:
    """Perform GET request to the specified m-Path API endpoint.

    If show_url=True, prints the fully-expanded URL *before* the request is sent (JWT redacted).
    """
    # Build the prepared request so we can show the exact URL first
    req = requests.Request("GET", f"{BASE_URL}/{endpoint}", params=params)
    prepped = req.prepare()
    if show_url:
        print(f"→ GET {_sanitize_url(prepped.url)}")

    # Send it
    with requests.Session() as s:
        resp = s.send(prepped, timeout=30)
    resp.raise_for_status()
    return resp.json()

def _stamp_and_dump(body: Dict, primary_key: str, out_dir: Path, suffix: str) -> List[Dict]:
    """Append download timestamp and save raw JSON payload."""
    utc_now = datetime.now(timezone.utc)
    iso_now = utc_now.strftime("%Y%m%dT%H%M%SZ")

    rows = body.get(primary_key, body.get("data", []))
    for r in rows:
        r["downloadedAt"] = iso_now

    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / f"{primary_key}_{suffix}_{iso_now}.json"
    out_json.write_text(json.dumps(body, indent=2, ensure_ascii=False))
    print(f"✓ Raw payload saved → {out_json}")
    return rows

# ─────────────────────────────────────────────── 4 | HIGH-LEVEL FETCH

def get_clients(user_code: str,
                changed_after_utc: Optional[str] = None,
                max_retries: int = 3,
                show_url: bool = False,
                include_changed_after: bool = True) -> Tuple[List[Dict], Path]:
    """Fetch clients list from m-Path and dump JSON only.

    Returns (rows, out_dir).
    """
    out_dir = BASE_DUMP_DIR

    # Ensure correct format if we are sending changedAfterUTC
    if include_changed_after:
        changed_after_utc = normalize_changed_after(changed_after_utc)
        suffix = changed_after_utc.replace(" ", "_").replace(":", "")
    else:
        changed_after_utc = None
        suffix = "all"

    for attempt in range(1, max_retries + 1):
        token = make_jwt(user_code=user_code)
        params = {
            "userCode": user_code,
            "JWT": token,
        }
        if include_changed_after and changed_after_utc:
            params["changedAfterUTC"] = changed_after_utc

        body = _call_raw("getClients", show_url=show_url, **params)

        status = body.get("status")
        if status == 1:
            rows = _stamp_and_dump(body, "clients", out_dir, suffix)
            return rows, out_dir

        if status == -1:
            if attempt < max_retries:
                print(f"API returned status –1 (attempt {attempt}/{max_retries}); retrying in 5 seconds.")
                time.sleep(5)
                continue
            raise RuntimeError("API gave status –1 after max retries.")

        raise RuntimeError(f"Unexpected API status: {status}\n{json.dumps(body, indent=2)}")


# ─────────────────────────────────────────────── 5 | USER CODE RESOLUTION

def resolve_user_code(cli_uc: Optional[str], auto_yes: bool = False) -> str:
    """Resolve which user_code to use.

    Priority:
      1. CLI value (if given)
      2. Environment variable MPATH_USERCODE (default 'rr7z8')
         - confirm interactively unless --yes or non-interactive
      3. Prompt user for input
    """
    if cli_uc:
        return cli_uc

    env_uc = os.getenv("MPATH_USERCODE", "rr7z8")
    if env_uc:
        if auto_yes or not sys.stdin.isatty():
            return env_uc
        ans = input(f"Use MPATH_USERCODE from env ({env_uc})? [Y/n] ").strip().lower()
        if ans in ("", "y", "yes"):
            return env_uc

    while True:
        uc = input("Enter your 5-character m-Path user code: ").strip()
        if len(uc) == 5:
            return uc
        print("Invalid length. Please enter exactly 5 characters.")

# ─────────────────────────────────────────────── 6 | CLI

def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Download client metadata from m-Path and save JSON (no CSV flattening)."
    )
    parser.add_argument("--user-code", help="5-char m-Path user code (overrides env).")
    parser.add_argument("--changed-after", "--since",
                        dest="changed_after",
                        default=None,
                        help='UTC datetime string, e.g. "2024-07-14 12:00:00" or just "2024-07-14".')
    parser.add_argument("--all", action="store_true",
                        help="Fetch everything (omit changedAfterUTC entirely).")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--yes", action="store_true",
                        help="Skip confirmation prompts (use env/default silently).")
    parser.add_argument("--show-url", action="store_true",
                        help="Print the fully-expanded request URL before it is sent (JWT redacted).")
    args = parser.parse_args()

    user_code = resolve_user_code(args.user_code, auto_yes=args.yes)

    rows, out_dir = get_clients(user_code=user_code,
                                changed_after_utc=args.changed_after,
                                max_retries=args.max_retries,
                                show_url=args.show_url,
                                include_changed_after=not args.all)

    print(f"Done. Rows: {len(rows)}")
    print(f"JSON files are in: {out_dir.resolve()}")


if __name__ == "__main__":
    _cli()
