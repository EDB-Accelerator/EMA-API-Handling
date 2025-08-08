#!/usr/bin/env python3
"""
get_clients.py – download raw client metadata from m-Path and save JSON files (no flattening)
Author: Kyunghun Lee (kyunghun.lee@nih.gov)

This module exposes two public entry points:
  • get_clients(...) – original high-level fetch that writes raw JSON payloads
  • get_client_ids_and_aliases(...) – convenience helper that returns [(connectionId, alias), ...]
      - accepts optional overrides (private_key_pem, base_dump_dir, base_url) without requiring MPathConfig
      - if base_dump_dir is omitted, nothing is written to disk

The CLI (python -m ema_api_handling.get_clients --help) still drives get_clients(...).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import jwt
import requests

__all__ = [
    "MPathConfig",
    "get_clients",
    "make_jwt",
    "normalize_changed_after",
    "resolve_user_code",
    "get_client_ids_and_aliases",  # NEW
]

# ─────────────────────────────────────────────── 0 | CONFIG

@dataclass(frozen=True)
class MPathConfig:
    """Container for paths and base URL.

    You can pass a custom instance into any high-level function (e.g., ``get_clients``)
    to override defaults when running from Jupyter or other scripts.
    """

    private_key_pem: Path = Path.home() / ".mpath_private_key.pem"
    public_key_pem: Path = Path.home() / ".mpath_public_key.pem"  # kept for parity/debugging
    base_dump_dir: Path = Path("mpath_clients").expanduser()
    base_url: str = "https://dashboard.m-path.io/API2"  # alt: "https://m-path.io/API2"


# Default values used unless you pass a custom config
GLOBAL_CONFIG = MPathConfig()
DEFAULT_CHANGED_AFTER_UTC = "2024-01-01 00:00:00"


# ─────────────────────────────────────────────── 1 | JWT

def make_jwt(user_code: str, ttl_minutes: int = 5, *, config: MPathConfig = GLOBAL_CONFIG) -> str:
    """Generate a signed JWT for m-Path authentication.

    Args:
        user_code: 5-character m-Path user code.
        ttl_minutes: Token lifetime in minutes.
        config: ``MPathConfig`` specifying the private key path.

    Returns:
        Encoded JWT string.
    """
    private_key = Path(config.private_key_pem).expanduser().read_text()
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    payload = {"exp": int(exp.timestamp()), "userCode": user_code}
    return jwt.encode(payload, private_key, algorithm="RS256")


# ─────────────────────────────────────────────── 2 | HELPERS

def normalize_changed_after(dt_str: Optional[str]) -> Optional[str]:
    """Normalize a user-supplied datetime string to 'YYYY-MM-DD HH:MM:SS' (UTC).

    Accepts either 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'. ``None`` or empty → default.
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
            "changedAfterUTC must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'. "
            f"Got: {dt_str}"
        )


def _sanitize_url(url: str) -> str:
    """Redact JWT token when printing URLs for logging/debugging."""
    if "JWT=" in url:
        head, tail = url.split("JWT=", 1)
        rest = tail.split("&", 1)
        if len(rest) == 2:
            _token, rest_params = rest
            return f"{head}JWT=<redacted>&{rest_params}"
        return f"{head}JWT=<redacted>"
    return url


def _call_raw(endpoint: str, *, show_url: bool = False, config: MPathConfig = GLOBAL_CONFIG, **params) -> Dict:
    """Perform GET request to the specified m-Path API endpoint.

    Args:
        endpoint: API method name (e.g., "getClients").
        show_url: If True, prints the fully-expanded URL (JWT redacted) before the request.
        config: ``MPathConfig`` providing the base URL.
        **params: Query parameters to be sent.

    Returns:
        Parsed JSON response as dict.
    """
    req = requests.Request("GET", f"{config.base_url}/{endpoint}", params=params)
    prepped = req.prepare()
    if show_url:
        print(f"→ GET {_sanitize_url(prepped.url)}")

    with requests.Session() as s:
        resp = s.send(prepped, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _stamp_and_dump(body: Dict, primary_key: str, out_dir: Path, suffix: str) -> List[Dict]:
    """Append download timestamp, then save raw JSON payload to disk.

    Args:
        body: Entire API response body.
        primary_key: Top-level key in the response that holds the data list ("clients" here).
        out_dir: Output directory.
        suffix: String to include in the filename (e.g. a timestamp or changedAfter value).

    Returns:
        The list of rows extracted from the body.
    """
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


# ─────────────────────────────────────────────── 3 | HIGH-LEVEL FETCH

def get_clients(
    user_code: str,
    changed_after_utc: Optional[str] = None,
    max_retries: int = 3,
    show_url: bool = False,
    include_changed_after: bool = True,
    *,
    config: MPathConfig = GLOBAL_CONFIG,
) -> Tuple[List[Dict], Path]:
    """Fetch client metadata from m-Path and dump raw JSON only.

    Args:
        user_code: 5-character m-Path user code.
        changed_after_utc: Filter by last-change UTC time (string). Use None for default.
        max_retries: Max # of retries when API returns status -1.
        show_url: Print expanded URL (JWT redacted) before sending.
        include_changed_after: If False, omit the changedAfterUTC parameter entirely (fetch all).
        config: ``MPathConfig`` overriding paths/URLs/output directory.

    Returns:
        (rows, out_dir): list of client records and the directory where JSON was written.
    """
    out_dir = config.base_dump_dir

    if include_changed_after:
        changed_after_utc = normalize_changed_after(changed_after_utc)
        suffix = changed_after_utc.replace(" ", "_").replace(":", "")
    else:
        changed_after_utc = None
        suffix = "all"

    for attempt in range(1, max_retries + 1):
        token = make_jwt(user_code=user_code, config=config)
        params = {"userCode": user_code, "JWT": token}
        if include_changed_after and changed_after_utc:
            params["changedAfterUTC"] = changed_after_utc

        body = _call_raw("getClients", show_url=show_url, config=config, **params)
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


# ─────────────────────────────────────────────── 4 | CONVENIENCE: IDs + ALIASES (NO MPathConfig REQUIRED)

def get_client_ids_and_aliases(
    user_code: str,
    changed_after_utc: str | None = None,
    *,
    private_key_pem: str | Path | None = None,
    base_url: str | None = None,
    base_dump_dir: str | Path | None = None,
    include_changed_after: bool = True,
    max_retries: int = 3,
    show_url: bool = False,
) -> list[tuple[int, str]]:
    """
    Returns a list of (connectionId, alias) tuples. Optionally writes raw JSON if base_dump_dir is given.

    This function does not require constructing MPathConfig:
      - private_key_pem: overrides GLOBAL_CONFIG.private_key_pem if provided
      - base_url: overrides GLOBAL_CONFIG.base_url if provided
      - base_dump_dir: if provided, enables dumping; if None, no files are written
    """
    # Build a one-off config from GLOBAL_CONFIG with optional overrides
    cfg = GLOBAL_CONFIG
    if any([private_key_pem, base_url, base_dump_dir]):
        cfg = replace(
            cfg,
            private_key_pem=Path(private_key_pem).expanduser() if private_key_pem else cfg.private_key_pem,
            base_url=base_url or cfg.base_url,
            base_dump_dir=Path(base_dump_dir).expanduser() if base_dump_dir else cfg.base_dump_dir,
        )

    # Prepare changedAfterUTC and dump suffix
    if include_changed_after:
        changed_after_utc = normalize_changed_after(changed_after_utc)
        suffix = changed_after_utc.replace(" ", "_").replace(":", "")
    else:
        changed_after_utc = None
        suffix = "all"

    # Decide whether to dump or not based on base_dump_dir presence
    do_dump = base_dump_dir is not None

    for attempt in range(1, max_retries + 1):
        token = make_jwt(user_code=user_code, config=cfg)
        params = {"userCode": user_code, "JWT": token}
        if include_changed_after and changed_after_utc:
            params["changedAfterUTC"] = changed_after_utc

        body = _call_raw("getClients", show_url=show_url, config=cfg, **params)
        status = body.get("status")

        if status == 1:
            rows = body.get("clients", body.get("data", []))

            # Attach timestamp + optionally dump
            iso_now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            for r in rows:
                r["downloadedAt"] = iso_now

            if do_dump:
                out_dir = cfg.base_dump_dir
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"clients_{suffix}_{iso_now}.json"
                out_file.write_text(json.dumps(body, indent=2, ensure_ascii=False))
                print(f"✓ Raw payload saved → {out_file}")

            return [(r.get("connectionId"), r.get("alias", "")) for r in rows]

        if status == -1:
            if attempt < max_retries:
                print(f"API returned status –1 (attempt {attempt}/{max_retries}); retrying in 5 seconds.")
                time.sleep(5)
                continue
            raise RuntimeError("API gave status –1 after max retries.")

        raise RuntimeError(f"Unexpected API status: {status}\n{json.dumps(body, indent=2)}")


# ─────────────────────────────────────────────── 5 | USER CODE RESOLUTION (CLI-ONLY)

def resolve_user_code(cli_uc: Optional[str], auto_yes: bool = False) -> str:
    """Resolve which user_code to use (CLI helper).

    Priority:
      1. CLI value if given
      2. $MPATH_USERCODE environment variable (default fallback 'rr7z8')
         - confirm unless --yes or stdin is non-interactive
      3. Prompt the user
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

    # Overrides for paths/URL/output directory
    parser.add_argument("--privkey", type=Path, help="Path to private key PEM.")
    parser.add_argument("--pubkey",  type=Path, help="Path to public  key PEM.")
    parser.add_argument("--base-url", type=str,  help="Base API URL (e.g. https://m-path.io/API2).")
    parser.add_argument("--outdir",   type=Path, help="Directory to dump raw JSON files.")

    args = parser.parse_args()

    user_code = resolve_user_code(args.user_code, auto_yes=args.yes)

    cfg = GLOBAL_CONFIG
    if any([args.privkey, args.pubkey, args.base_url, args.outdir]):
        cfg = replace(
            cfg,
            private_key_pem=args.privkey or cfg.private_key_pem,
            public_key_pem=args.pubkey or cfg.public_key_pem,
            base_url=args.base_url or cfg.base_url,
            base_dump_dir=args.outdir or cfg.base_dump_dir,
        )

    rows, out_dir = get_clients(
        user_code=user_code,
        changed_after_utc=args.changed_after,
        max_retries=args.max_retries,
        show_url=args.show_url,
        include_changed_after=not args.all,
        config=cfg,
    )

    print(f"Done. Rows: {len(rows)}")
    print(f"JSON files are in: {out_dir.resolve()}")


if __name__ == "__main__":
    _cli()
