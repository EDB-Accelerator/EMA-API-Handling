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
DEFAULT_PRIVATE_KEY_PEM = Path.home() / ".mpath_private_key.pem"
DEFAULT_BASE_DUMP_DIR = Path("mpath_raw").expanduser()

# ─────────────────────────────────────────────── 2 | JWT
def make_jwt(user_code, ttl_minutes: int = 5, private_key_path: Path = DEFAULT_PRIVATE_KEY_PEM) -> str:
    """
    Generate a signed JWT for user authentication.
    
    Args:
        user_code (str): 5-character m-Path user code.
        ttl_minutes (int): Token expiration in minutes.

    Returns:
        str: Encoded JWT string.
    """
    # private_key = DEFAULT_PRIVATE_KEY_PEM.read_text()
    # private_key = DEFAULT_PRIVATE_KEY_PEM.read_text()
    key_path = Path(private_key_path).expanduser()
    if not key_path.exists():
        raise FileNotFoundError(f"Private key not found at: {key_path}")
    private_key = key_path.read_text()

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

# ─────────────────────────────────────────────── A | ONE-TAB JSON→CSV (FORMAL)
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import json, time
import pandas as pd

# Utilities already used elsewhere
def _to_scalar(val):
    return json.dumps(val, ensure_ascii=False) if isinstance(val, (list, dict)) else val

def _flatten_answer(ans: dict, rec: dict, prefix: str):
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
    flat = []
    for entry in raw_rows:
        row = {}
        for k, v in entry.items():
            if k != "data":
                row[k] = _to_scalar(v)
        inner = entry.get("data", {})
        for k, v in inner.items():
            if k != "answers":
                row[f"data_{k}"] = _to_scalar(v)
        for ans in inner.get("answers", []):
            sq = ans.get("basicQuestion", {}).get("shortQuestion", "Q")
            _flatten_answer(ans, row, f"{sq}_")
        flat.append(row)
    return pd.DataFrame(flat)

def _load_rows_from_json(json_path: Path) -> list[dict]:
    if not json_path.exists():
        raise FileNotFoundError(f"JSON not found: {json_path}")
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    if isinstance(obj, dict) and "data" in obj:
        return obj.get("data", [])
    if isinstance(obj, list):
        return obj
    raise ValueError("Unrecognized JSON structure (expected dict with 'data' or list).")

def _convert_timestamp_columns(df: pd.DataFrame,
                               tz_str: str = "US/Eastern",
                               origin: str = "local") -> pd.DataFrame:
    """
    Convert ms-epoch columns named like *timeStamp*/*timestamp*.
    origin='local': treat ms as local wall time -> tz_localize (no shift)
    origin='utc'  : treat ms as UTC -> tz_convert (shift to local)
    """
    cand = [c for c in df.columns if ("timeStamp" in c or "timestamp" in c)]
    num  = [c for c in cand if pd.api.types.is_numeric_dtype(df[c])]
    if not num:
        return df

    for c in num:
        try:
            if origin == "utc":
                dt = pd.to_datetime(df[c], unit="ms", utc=True).dt.tz_convert(tz_str)
            else:  # origin == "local"
                dt = pd.to_datetime(df[c], unit="ms").dt.tz_localize(
                    tz_str, nonexistent="shift_forward", ambiguous="NaT"
                )
            df[c] = dt.dt.strftime("%Y-%m-%d %H:%M:%S%z")
        except Exception as e:
            print(f"[WARN] Failed to convert '{c}': {e}")
    return df

def json_to_clean_csv(json_path: Path,
                      local_tz: str = "US/Eastern",
                      timestamp_origin: str = "local",
                      out_dir: Path | None = None,
                      out_name: str | None = None
                     ) -> tuple[pd.DataFrame, Path]:
    """
    Load m-Path JSON (dict with 'data' or list), flatten to a DataFrame,
    convert timestamp columns with the requested semantics, write CSV, and return (df, csv_path).
    """
    raw_rows = _load_rows_from_json(json_path)
    df = flatten_rows(raw_rows)
    df = _convert_timestamp_columns(df, tz_str=local_tz, origin=timestamp_origin)

    # Output path default: sibling to JSON, using your "__clean_{N}rows.csv" convention
    out_dir = Path(out_dir) if out_dir else json_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    if out_name:
        csv_path = out_dir / out_name
    else:
        # Keep your convention e.g., "data_294502_20250820T160619Z__clean_123rows.csv"
        stem = json_path.stem  # e.g., "data_294502_20250820T160619Z"
        csv_path = out_dir / f"{stem}__clean_{len(df)}rows.csv"

    df.to_csv(csv_path, index=False)
    print(f"✓ Clean CSV saved → {csv_path}")
    return df, csv_path


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
