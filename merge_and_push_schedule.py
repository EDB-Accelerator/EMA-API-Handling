#!/usr/bin/env python3
# merge_and_push_schedule.py – merge new m-Path beeps with existing schedule and push back to server
# Author: Kyunghun Lee (kyunghun.lee@nih.gov)
# Updated: 2025-07-01
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

"""
merge_and_push_schedule.py

- Downloads existing schedule for a connection
- Merges new entries with existing ones
- Pushes updated schedule back to m-Path server

Example usage (notebook):
>>> from merge_and_push_schedule import build_entries, merge_and_push
>>> new = build_entries(
...         starts  = ["2025-07-24 09:00:00"],
...         ends    = ["2025-07-24 10:00:00"],
...         item_id = "N1m7ygNkbTTi6N8D",
...         labels  = ["jul24_morning2"])
>>> merge_and_push(connection_id=290982, new_entries=new)
"""

from __future__ import annotations

import json, math, os, time, jwt, requests
from datetime import datetime, timedelta, timezone
from itertools import count
from pathlib import Path
from typing import Sequence

# ───────────────────────────────────────────── 0 | CONFIGURATION
BASE_URL = "https://m-path.io/API2"
DEFAULT_USER_CODE = os.getenv("MPATH_USERCODE")
DEFAULT_private_key_path = Path(os.getenv("MPATH_PRIVKEY", Path.home() / ".mpath_private_key.pem"))


# ───────────────────────────────────────────── 1 | JWT GENERATOR
def _jwt(user_code: str, private_key_path: Path, ttl_min: int = 5) -> str:
    """Generate a short-lived JWT for authenticated API calls."""
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl_min)
    payload = {"exp": int(exp.timestamp()), "userCode": user_code}
    return jwt.encode(payload, private_key_path.read_text(), algorithm="RS256")

# ───────────────────────────────────────────── 2 | API CALLS
def _fetch_schedule(connection_id: int, user_code: str, private_key_path: Path) -> list[dict]:
    """Fetch the existing schedule for a given connection."""
    params = {"userCode": user_code, "connectionId": connection_id, "JWT": _jwt(user_code, private_key_path)}
    body = requests.get(f"{BASE_URL}/getSchedule", params=params, timeout=30).json()
    if body.get("status") != 1:
        raise RuntimeError(f"getSchedule failed: {body}")
    return body["schedule"]

def _push_schedule(connection_id: int, entries: list[dict],
                   user_code: str, private_key_path: Path, retries=3) -> dict:
    """Push the updated schedule (merged) to m-Path."""
    params = {"userCode": user_code, "connectionId": connection_id, "JWT": _jwt(user_code, private_key_path)}
    data = {"scheduleJSON": json.dumps(entries, ensure_ascii=False)}

    for attempt in range(1, retries + 1):
        r = requests.post(f"{BASE_URL}/setSchedule", params=params, data=data, timeout=30)
        r.raise_for_status()
        reply = r.json()
        if reply.get("status") != -1:
            if reply.get("status") != 1:
                raise RuntimeError(f"setSchedule rejected: {reply}")
            return reply
        if attempt == retries:
            raise RuntimeError("setSchedule kept returning status –1.")
        time.sleep(2 * attempt)

# ───────────────────────────────────────────── 3 | ENTRY CONSTRUCTION
_WHITELIST = {
    "startTime", "endTime", "scheduledTime",
    "itemId", "beepId", "localId",
    "expirationInterval", "reminderIntervals",
    "randomizationScheme"
}

def build_entries(
    *,
    starts: Sequence[str],
    ends: Sequence[str | None],
    item_id: str,
    labels: Sequence[str] | None = None,
    expiration_interval: int | None = None,
    reminder_intervals: Sequence[int] | None = None,
    randomization_scheme: int = 0,
) -> list[dict]:
    """
    Construct a list of new schedule entries.

    Args:
        starts: List of local start time strings.
        ends: List of corresponding end time strings.
        item_id: Survey or item ID.
        labels: Optional list of localId labels.
        expiration_interval: Optional fallback if endTime is missing.
        reminder_intervals: Optional reminder delay(s).
        randomization_scheme: Optional randomization type.

    Returns:
        List of schedule dictionaries ready to push.
    """
    if len(starts) != len(ends):
        raise ValueError("starts and ends length mismatch")

    labels = list(labels or [])
    gen = count(1)
    while len(labels) < len(starts):
        labels.append(f"auto_{next(gen)}")

    new: list[dict] = []
    for st, et, lid in zip(starts, ends, labels):
        e = {
            "startTime": st,
            "scheduledTime": st,
            "itemId": item_id,
            "beepId": 0,
            "localId": lid,
            "randomizationScheme": randomization_scheme,
        }
        if et:
            e["endTime"] = et
        elif expiration_interval is not None:
            e["expirationInterval"] = expiration_interval
        else:
            raise ValueError("Need endTime or expiration_interval.")

        if reminder_intervals:
            e["reminderIntervals"] = list(reminder_intervals)

        new.append(e)
    return new

# ───────────────────────────────────────────── 4 | CLEAN EXISTING ROWS
def _clean(rec: dict) -> dict:
    """Remove extraneous keys and normalize values."""
    out = {}
    for k, v in rec.items():
        if k not in _WHITELIST:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        if isinstance(v, str) and v.startswith('[') and v.endswith(']'):
            try:
                v = json.loads(v)
            except Exception:
                pass
        out[k] = v
    return out

# ───────────────────────────────────────────── 5 | MERGE AND PUSH
def merge_and_push(*,
                   connection_id: int,
                   new_entries: list[dict],
                   user_code: str = DEFAULT_USER_CODE,
                   private_key_path: Path = DEFAULT_private_key_path) -> dict:
    """
    Merge new entries with current schedule and push result.

    Args:
        connection_id: Target m-Path connection ID.
        new_entries: List of entries created by build_entries.
        user_code: 5-char practitioner code.
        private_key_path: Path to PEM private key file.

    Returns:
        API response from setSchedule.
    """
    if not user_code:
        raise ValueError("user_code is required (or set MPATH_USERCODE env variable).")
    if not private_key_path.exists():
        raise FileNotFoundError(f"RSA private key not found: {private_key_path}")

    current = [_clean(r) for r in _fetch_schedule(connection_id, user_code, private_key_path)]
    return _push_schedule(connection_id, current + new_entries, user_code, private_key_path)


# ───────────────────────────────────────────── 6 | CLI DEMO (OPTIONAL)
if __name__ == "__main__":
    import argparse, json
    cli = argparse.ArgumentParser(description="Demo: add beeps without loss.")
    cli.add_argument("--connection", type=int, required=True)
    cli.add_argument("--item", required=True, help="itemId")
    args = cli.parse_args()

    demo = build_entries(
        starts=["2025-07-25 09:00:00"],
        ends=["2025-07-25 10:00:00"],
        item_id=args.item,
        labels=["jul25_demo"],
    )
    res = merge_and_push(connection_id=args.connection, new_entries=demo)
    print(json.dumps(res, indent=2))
