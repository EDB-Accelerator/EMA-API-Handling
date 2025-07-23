#!/usr/bin/env python3
# set_schedule_from_json.py – Upload a schedule JSON file to m-Path
# Author: Kyunghun Lee (kyunghun.lee@nih.gov)
# Updated: 2025-07-23
#
# MIT License
# Copyright (c) 2025 Kyunghun Lee
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from __future__ import annotations

import json, os, sys, time, jwt, requests, argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Any

BASE_URL = "https://m-path.io/API2"

# ─────────────────────────────────────────────── 0 | ALLOWED SCHEDULE KEYS
_KEEP_KEYS = {
    "startTime", "endTime", "scheduledTime",
    "itemId", "beepId", "localId",
    "randomizationScheme", "reminderIntervals",
    "expirationInterval", "useAsButton", "singleUse",
    "required", "passed", "scheduleType",
}

def _minimalize(entries: Iterable[dict]) -> List[dict]:
    """Strip unknown keys so the payload matches the /setSchedule schema."""
    return [{k: v for k, v in e.items() if k in _KEEP_KEYS} for e in entries]

# ─────────────────────────────────────────────── 1 | JWT HELPER
def _jwt(user_code: str, privkey: Path, ttl_min: int = 5) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl_min)
    return jwt.encode(
        {"exp": int(exp.timestamp()), "userCode": user_code},
        privkey.read_text(),
        algorithm="RS256",
    )

# ─────────────────────────────────────────────── 2 | CORE FUNCTION
def set_schedule(
    entries: List[dict],
    user_code: str,
    connection_id: int,
    privkey_path: str | os.PathLike,
    *,
    minimal: bool = False,
    retries: int = 3,
) -> dict:
    """
    Upload the schedule to m-Path via the /setSchedule endpoint.
    """
    if minimal:
        entries = _minimalize(entries)

    privkey = Path(privkey_path).expanduser().resolve()
    if not privkey.is_file():
        raise FileNotFoundError(f"RSA private key not found: {privkey}")

    params = dict(
        userCode     = user_code,
        connectionId = connection_id,
        JWT          = _jwt(user_code, privkey),
        scheduleJSON = json.dumps(entries, ensure_ascii=False),
    )

    for attempt in range(1, retries + 1):
        r = requests.post(f"{BASE_URL}/setSchedule", params=params, timeout=30)
        r.raise_for_status()
        try:
            body = r.json()
        except ValueError:
            raise RuntimeError("Server returned non-JSON:\n" + r.text)

        if body.get("status") != -1:
            return body
        if attempt == retries:
            raise RuntimeError("Server returned status –1 repeatedly; upload failed.")
        print(f"Retrying due to status –1 [{attempt}/{retries}] …")
        time.sleep(2 * attempt)

# ─────────────────────────────────────────────── 3 | CLI ENTRY POINT
def _cli() -> None:
    ap = argparse.ArgumentParser(description="Upload a schedule JSON to m-Path.")
    ap.add_argument("json_file",        help="Path to schedule_….json")
    ap.add_argument("--user_code",      help="Practitioner code (overrides env)")
    ap.add_argument("--connection_id",  type=int, help="Connection/participant ID")
    ap.add_argument("--privkey",        help="Path to RSA private key PEM")
    ap.add_argument("--minimal",        action="store_true",
                    help="Strip unknown keys before upload")
    ap.add_argument("--retries", type=int, default=3,
                    help="Number of retries on status –1 (default: 3)")
    args = ap.parse_args()

    # Credentials
    user_code = args.user_code or os.getenv("MPATH_USERCODE") \
                or input("Enter USER CODE: ").strip()

    conn_id = args.connection_id or (
        int(os.getenv("MPATH_CONNECTION_ID"))
        if os.getenv("MPATH_CONNECTION_ID", "").isdigit()
        else int(input("Enter CONNECTION ID: ").strip())
    )

    privkey = args.privkey or os.getenv("MPATH_PRIVKEY") \
              or Path.home() / ".mpath_private_key.pem"

    # Load JSON file
    jpath = Path(args.json_file).expanduser().resolve()
    try:
        entries = json.loads(jpath.read_text("utf-8"))
    except Exception as e:
        sys.exit(f"Failed to read/parse JSON: {e}")

    if not isinstance(entries, list) or not entries:
        sys.exit("JSON must be a non-empty list of objects.")

    print(f"Uploading {len(entries)} entr{'y' if len(entries)==1 else 'ies'} …")
    reply = set_schedule(entries, user_code, conn_id, privkey,
                         minimal=args.minimal, retries=args.retries)
    print(json.dumps(reply, indent=2, ensure_ascii=False))
    if "new2id" in reply:
        print("\nMapping localId to new beepId:")
        print(json.dumps(reply["new2id"], indent=2))

if __name__ == "__main__":
    _cli()
