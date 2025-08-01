#!/usr/bin/env python3
# get_schedule.py – download and flatten m-Path schedule data for a single connection
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
get_schedule.py

Fetch and flatten schedule entries for one m-Path connection.

Outputs:
- Raw JSON file
- Single CSV file with flattened schedule rows

Usage examples:
CLI:  python get_schedule.py --connection_id 123456
API:  import get_schedule as mp
      df = mp.get_schedule(connection_id=123456, user_code="ukmp2")
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, timezone
import argparse, json, os, re, sys, time, requests, jwt
import pandas as pd

# ───────────────────────────────────────────── 0 | PATHS & CONSTANTS
BASE_URL = "https://m-path.io/API2"
DEFAULT_PRIVKEY_PATH = Path.home() / ".mpath_private_key.pem"
DEFAULT_BASE_OUT = Path("schedule_raw").expanduser()

# ───────────────────────────────────────────── 1 | LOW-LEVEL HELPERS
def _make_jwt(user_code: str, ttl_min: int = 5, privkey_path: Path = DEFAULT_PRIVKEY_PATH) -> str:
    """Generate a short-lived signed JWT token for m-Path API."""
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl_min)
    payload = {"exp": int(exp.timestamp()), "userCode": user_code}
    return jwt.encode(payload, privkey_path.read_text(), algorithm="RS256")

def _to_scalar(v):
    """Convert list or dict to JSON string for CSV compatibility."""
    return json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v

def _flatten(obj: dict, parent_key: str = "", sep: str = ".") -> dict:
    """Flatten nested dictionaries using dot notation."""
    out = {}
    for k, v in obj.items():
        key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            out.update(_flatten(v, key, sep=sep))
        else:
            out[key] = _to_scalar(v)
    return out

# ───────────────────────────────────────────── 2 | API FETCH
def _fetch_schedule(user_code: str, connection_id: int, retries: int = 3,
                    private_key_path: Path = DEFAULT_PRIVKEY_PATH) -> list[dict]:
    """Fetch schedule entries from m-Path API, retry on status -1."""
    for attempt in range(1, retries + 1):
        params = {
            "userCode": user_code,
            "connectionId": connection_id,
            "JWT": _make_jwt(user_code, privkey_path=private_key_path)
        }
        body = requests.get(f"{BASE_URL}/getSchedule", params=params, timeout=30).json()

        status = body.get("status")
        if status == 1:
            return body.get("schedule", [])
        if status == -1 and attempt < retries:
            print(f"status –1; retrying … [{attempt}/{retries}]")
            time.sleep(5)
            continue
        raise RuntimeError(f"API error:\n{json.dumps(body, 2)}")


# ───────────────────────────────────────────── 3 | SAVE / CONVERT
_TS_FMT = "%Y-%m-%d %H:%M:%S"

def _stamp_and_dump(raw_obj, stem: str, out_dir: Path) -> str:
    """Save raw JSON with a timestamped filename."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fp = out_dir / f"{stem}_{ts}.json"
    fp.write_text(json.dumps(raw_obj, indent=2, ensure_ascii=False))
    print(f"✓ Raw JSON saved → {fp}")
    return ts

def _save_schedule(entries: list[dict], connection_id: int, out_dir: Path,
                   tz: str = "US/Eastern") -> pd.DataFrame:
    """
    Flatten schedule entries and save as CSV.

    Args:
        entries: List of schedule dictionaries.
        connection_id: Participant's connection ID.
        out_dir: Directory to save the output files.
        tz: Timezone for timestamp conversion.

    Returns:
        Flattened pandas DataFrame of the schedule.
    """
    ts = _stamp_and_dump(entries, f"schedule_{connection_id}", out_dir)

    if not entries:
        print("Empty schedule.")
        return pd.DataFrame()

    df = pd.DataFrame([_flatten(e) for e in entries])

    # Convert timestamp fields to local time
    ts_cols = [c for c in df.columns
               if ("timeStamp" in c or "timeStart" in c or "timeEnd" in c)
               and df[c].dtype != "object"]
    if ts_cols:
        df[ts_cols] = (
            pd.to_datetime(df[ts_cols].stack(), unit="ms", utc=True)
              .dt.tz_convert(tz)
              .dt.strftime(_TS_FMT)
              .unstack()
        )

    fp = out_dir / f"schedule_{connection_id}_{ts}_{len(df)}rows.csv"
    df.to_csv(fp, index=False)
    print(f"✓ CSV saved → {fp}")
    return df

# ───────────────────────────────────────────── 4 | PUBLIC ENTRY POINT
def get_schedule(*, connection_id: int | None = None,
                 user_code: str | None = None,
                 retries: int = 3,
                 out_base: Path | str = DEFAULT_BASE_OUT,
                 private_key_path: Path = DEFAULT_PRIVKEY_PATH
                ) -> pd.DataFrame:
    """
    Fetch and save schedule data for a single m-Path connection.

    Parameters:
        connection_id: Participant's connection ID.
        user_code: 5-character practitioner code.
        retries: Number of retry attempts for status –1.
        out_base: Output directory.
        privkey_path: Path to RSA private key.

    Returns:
        Flattened pandas DataFrame with one row per schedule entry.
    """
    user_code = user_code or os.getenv("MPATH_USERCODE")
    if not user_code:
        raise ValueError("MPATH_USERCODE not set and user_code parameter missing.")

    if connection_id is None:
        env_id = os.getenv("MPATH_CONNECTION_ID")
        if env_id and env_id.isdigit():
            connection_id = int(env_id)
        else:
            connection_id = int(input("Enter numeric CONNECTION ID: ").strip())

    if not private_key_path.exists():
        raise FileNotFoundError(f"RSA private key not found: {private_key_path}")

    out_dir = Path(out_base) / str(connection_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching schedule for connection {connection_id} …")
    entries = _fetch_schedule(user_code, connection_id, retries=retries, private_key_path=private_key_path)
    return _save_schedule(entries, connection_id, out_dir)


# ───────────────────────────────────────────── 5 | CLI HANDLER
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download m-Path schedule.")
    parser.add_argument("--connection_id", type=int, help="Connection/participant ID")
    parser.add_argument("--user_code", type=str, help="5-char practitioner code (overrides env)")
    args, _ = parser.parse_known_args()

    try:
        get_schedule(connection_id=args.connection_id, user_code=args.user_code)
    except Exception as e:
        sys.exit(f"Failed: {e}")
