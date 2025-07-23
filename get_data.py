#!/usr/bin/env python3
# get_data.py – download raw m-Path data and write a fully-flattened CSV
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

from __future__ import annotations
from pathlib import Path
from datetime import datetime, timedelta, timezone
import json, os, requests, jwt, time
import pandas as pd

# ─────────────────────────────────────────────── 0 | SETTINGS
# Define key file paths and base output directory
PRIVATE_KEY_PEM = Path.home() / ".mpath_private_key.pem"
PUBLIC_KEY_PEM = Path.home() / ".mpath_public_key.pem"
BASE_DUMP_DIR = Path("mpath_raw").expanduser()

# ─────────────────────────────────────────────── 2 | JWT
def make_jwt(user_code, ttl_minutes: int = 5) -> str:
    """
    Generate a signed JWT for user authentication.
    
    Args:
        user_code (str): 5-character m-Path user code.
        ttl_minutes (int): Token expiration in minutes.

    Returns:
        str: Encoded JWT string.
    """
    private_key = PRIVATE_KEY_PEM.read_text()
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    payload = {"exp": int(exp.timestamp()), "userCode": user_code}
    return jwt.encode(payload, private_key, algorithm="RS256")

# ─────────────────────────────────────────────── 3 | API HELPERS
def _call_raw(endpoint: str, **params) -> dict:
    """
    Perform GET request to the specified m-Path API endpoint.

    Args:
        endpoint (str): API method name.
        **params: Query parameters.

    Returns:
        dict: Parsed JSON response.
    """
    BASE_URL = "https://m-path.io/API2"
    resp = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def _stamp_and_dump(body: dict, key: str, connection_id: int, conn_dir: Path) -> list[dict]:
    """
    Append download timestamp and save raw JSON payload.

    Args:
        body (dict): API response body.
        key (str): Key to extract from the response.
        connection_id (int): Target connection ID.
        conn_dir (Path): Output directory.

    Returns:
        list[dict]: List of data rows.
    """
    utc_now = datetime.now(timezone.utc)
    iso_now = utc_now.strftime("%Y%m%dT%H%M%SZ")
    for row in body.get(key, []):
        row["downloadedAt"] = iso_now
    out_json = conn_dir / f"{key}_{connection_id}_{iso_now}.json"
    out_json.write_text(json.dumps(body, indent=2, ensure_ascii=False))
    print(f"✓ Raw payload saved → {out_json}")
    return body[key]

def get_data(user_code=None, connection_id=None, max_retries: int = 3) -> tuple[list[dict], Path]:
    """
    Fetch raw data from m-Path API with retry logic.

    Args:
        user_code (str): m-Path user code.
        connection_id (int): Connection ID to retrieve data for.
        max_retries (int): Number of retry attempts on failure.

    Returns:
        tuple: (List of raw data rows, output directory path)
    """
    conn_dir = BASE_DUMP_DIR / str(connection_id)
    conn_dir.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, max_retries + 1):
        token = make_jwt(user_code=user_code)
        body = _call_raw("getData", userCode=user_code, JWT=token, connectionId=connection_id)

        status = body.get("status")
        if status == 1:
            return _stamp_and_dump(body, "data", connection_id, conn_dir), conn_dir

        if status == -1:
            if attempt < max_retries:
                print(f"API returned status –1 (attempt {attempt}/{max_retries}); retrying in 5 seconds.")
                time.sleep(5)
                continue
            raise RuntimeError("API gave status –1 after max retries.")
        raise RuntimeError(f"Unexpected API status: {status}\n{json.dumps(body, 2)}")

# ─────────────────────────────────────────────── 4 | FLATTEN UTILITIES
def _to_scalar(val):
    """Convert list or dict to JSON string, leave scalars unchanged."""
    return json.dumps(val, ensure_ascii=False) if isinstance(val, (list, dict)) else val

def _flatten_answer(ans: dict, rec: dict, prefix: str):
    """
    Flatten a single answer block into a flat dictionary format.
    
    Recursively handles nested containerAnswer structures.
    """
    for k, v in ans.items():
        if k in ("basicQuestion", "cAnswer"):
            continue
        rec[f"{prefix}{k}"] = _to_scalar(v)

    bq = ans.get("basicQuestion", {})
    for subk, subv in bq.items():
        rec[f"{prefix}basicQuestion_{subk}"] = _to_scalar(subv)

    for valkey in ("iAnswer", "dAnswer", "sAnswer"):
        if valkey in ans and ans[valkey]:
            rec[f"{prefix}value"] = ans[valkey][0]
            break

    if ans.get("typeAnswer") == "containerAnswer":
        for child in ans.get("cAnswer", []):
            child_sq = child.get("basicQuestion", {}).get("shortQuestion", "container")
            _flatten_answer(child, rec, f"{prefix}{child_sq}_")

def flatten_rows(raw_rows: list[dict]) -> pd.DataFrame:
    """
    Convert list of raw m-Path rows to a flat tabular DataFrame.

    Args:
        raw_rows (list): Raw JSON response entries.

    Returns:
        pd.DataFrame: Flattened DataFrame.
    """
    flattened = []
    for entry in raw_rows:
        row: dict = {}

        for k, v in entry.items():
            if k != "data":
                row[k] = _to_scalar(v)

        inner = entry["data"]
        for k, v in inner.items():
            if k != "answers":
                row[f"data_{k}"] = _to_scalar(v)

        for ans in inner.get("answers", []):
            sq = ans.get("basicQuestion", {}).get("shortQuestion", "Q")
            _flatten_answer(ans, row, f"{sq}_")

        flattened.append(row)
    return pd.DataFrame(flattened)

def flatten_and_save(raw_rows: list[dict], connection_id: int,
                     conn_dir: Path, tz: str = "US/Eastern"
                    ) -> tuple[pd.DataFrame, Path]:
    """
    Flatten raw rows, localize timestamps, and save to CSV.

    Args:
        raw_rows (list): List of raw JSON records.
        connection_id (int): Participant's connection ID.
        conn_dir (Path): Output directory.
        tz (str): Timezone string for conversion.

    Returns:
        tuple: (Flattened DataFrame, path to saved CSV file)
    """
    df = flatten_rows(raw_rows)

    ts_cols = [c for c in df.columns
               if ("timeStamp" in c) and df[c].dtype != "object"]
    if ts_cols:
        df[ts_cols] = (
            pd.to_datetime(df[ts_cols].stack(), unit="ms", utc=True)
              .dt.tz_convert(tz)
              .dt.strftime("%Y-%m-%d %H:%M:%S")
              .unstack()
        )

    iso_now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    csv_path = conn_dir / f"data_clean_{connection_id}_{iso_now}_{len(df)}rows.csv"
    df.to_csv(csv_path, index=False)
    print(f"✓ Clean CSV saved → {csv_path}")
    return df, csv_path
