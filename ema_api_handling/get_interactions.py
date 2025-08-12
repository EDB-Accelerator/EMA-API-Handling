#!/usr/bin/env python3
# get_interactions.py – download and flatten m-Path interaction data per root container
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
get_interactions.py

Fetch and flatten interactions for one m-Path connection.

Outputs:
- Raw JSON file
- One CSV per root container (flattened questions)

Usage:
CLI:  python get_interactions.py --connection_id 123456
API:  import get_interactions as mp
      mp.get_interactions(connection_id=123456, user_code="ukmp2")
"""

from __future__ import annotations
from pathlib import Path
from datetime import datetime, timedelta, timezone
import argparse, json, os, re, sys, time, requests, jwt
import pandas as pd

# ───────────────────────────────────────────── 0 | PATHS & CONSTANTS
BASE_URL = "https://m-path.io/API2"
DEFAULT_PRIVKEY_PATH = Path.home() / ".mpath_private_key.pem"
DEFAULT_BASE_OUT = Path("interactions_raw").expanduser()

# ───────────────────────────────────────────── 1 | LOW-LEVEL HELPERS
def _make_jwt(user_code: str, ttl_min: int = 5, privkey_path: Path = DEFAULT_PRIVKEY_PATH) -> str:
    """Generate a short-lived signed JWT token."""
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl_min)
    payload = {"exp": int(exp.timestamp()), "userCode": user_code}
    return jwt.encode(payload, privkey_path.read_text(), algorithm="RS256")

def _to_scalar(v):
    """Convert lists and dicts to JSON strings for CSV compatibility."""
    return json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v

# ───────────────────────────────────────────── 2 | API REQUEST
# def _fetch_interactions(user_code: str, connection_id: int, retries: int = 3,
#                         privkey_path: Path = DEFAULT_PRIVKEY_PATH) -> list[dict]:
#     """Fetch interaction data from the API with retry on status –1."""
#     for attempt in range(1, retries + 1):
#         params = {
#             "userCode": user_code,
#             "connectionId": connection_id,
#             "JWT": _make_jwt(user_code, privkey_path=privkey_path)
#         }
#         body = requests.get(f"{BASE_URL}/getInteractions", params=params, timeout=30).json()

#         status = body.get("status")
#         if status == 1:
#             return body.get("interactions", [])
#         if status == -1 and attempt < retries:
#             print(f"status –1; retrying … [{attempt}/{retries}]")
#             time.sleep(5)
#             continue
#         # raise RuntimeError(f"API error:\n{json.dumps(body, 2)}")
#         raise RuntimeError(f"API error:\n{json.dumps(body, indent=2)}")

def _fetch_interactions(user_code: str, connection_id: int, retries: int = 10,
                        privkey_path: Path = DEFAULT_PRIVKEY_PATH) -> list[dict]:
    """Fetch interaction data with exponential backoff and clearer errors."""
    backoff = 3  # seconds
    for attempt in range(1, retries + 1):
        params = {
            "userCode": user_code,
            "connectionId": connection_id,
            "JWT": _make_jwt(user_code, privkey_path=privkey_path),
        }

        try:
            resp = requests.get(f"{BASE_URL}/getInteractions", params=params, timeout=30)
        except requests.RequestException as e:
            if attempt == retries:
                raise RuntimeError(f"Network error after {retries} attempts: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

        # Try JSON; if not JSON, raise a helpful error
        try:
            body = resp.json()
        except ValueError:
            raise RuntimeError(
                f"API error (non-JSON response): HTTP {resp.status_code}\n{resp.text[:500]}"
            )

        status = body.get("status")

        if status == 1:
            return body.get("interactions", [])

        if status == -1:
            # transient/processing – retry
            if attempt < retries:
                wait = backoff
                print(f"status –1; retrying in {wait}s … [{attempt}/{retries}]")
                time.sleep(wait)
                backoff = min(backoff * 2, 60)
                continue
            # ran out of retries
            raise RuntimeError(
                "API kept returning status –1 (transient). "
                "Server may still be processing or throttling. Try again later or increase retries."
            )

        # Any other status → include body for debugging
        raise RuntimeError(
            "Unexpected API status: "
            f"{status}\n{json.dumps(body, indent=2, ensure_ascii=False)}"
        )


# ───────────────────────────────────────────── 3 | FLATTEN TREE
def _walk(item: dict, path: list[str], rows: list[dict]):
    """Recursively walk tree structure and flatten questions."""
    cur_path = path + [item.get("shortQuestion") or item.get("itemId", "")]
    if item.get("typeQuestion") == "container":
        for child in item.get("items", []):
            _walk(child, cur_path, rows)
        return

    rec = {"path": "/".join(p for p in cur_path if p)}
    for k, v in item.items():
        if k == "items":
            continue
        if isinstance(v, dict):
            for subk, subv in v.items():
                rec[f"{k}.{subk}"] = _to_scalar(subv)
        else:
            rec[k] = _to_scalar(v)
    rows.append(rec)

def _questions_df(root: dict) -> pd.DataFrame:
    """Convert a single root container into a flattened DataFrame."""
    rows: list[dict] = []
    _walk(root, [], rows)
    return pd.DataFrame(rows)

# ───────────────────────────────────────────── 4 | SAVE OUTPUTS
def _slug(text: str, maxlen: int = 48) -> str:
    """Sanitize title into a valid filename slug."""
    return re.sub(r"[^\w\-]+", "_", text.strip())[:maxlen] or "root"

def _stamp_and_dump(raw_obj, stem: str, out_dir: Path) -> str:
    """Save raw JSON with timestamped filename."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fp = out_dir / f"{stem}_{ts}.json"
    fp.write_text(json.dumps(raw_obj, indent=2, ensure_ascii=False))
    print(f"✓ Raw JSON saved → {fp}")
    return ts

def _flatten_and_save_roots(roots: list[dict], connection_id: int, out_dir: Path,
                            tz: str = "US/Eastern") -> dict[str, pd.DataFrame]:
    """
    Flatten each root container and save CSV per root.
    Also saves:
      • interactions_<connection_id>_<timestamp>.json  (all roots, raw)
      • <idx>_<slug>_<timestamp>_raw.json             (per-root, raw)
    """
    ts = _stamp_and_dump(roots, f"interactions_{connection_id}", out_dir)
    dfs: dict[str, pd.DataFrame] = {}

    if not roots:
        print("No interactions returned.")
        return dfs

    for idx, root in enumerate(roots, 1):
        title = (root.get("fullQuestion")
                 or root.get("shortQuestion")
                 or root.get("itemId")
                 or f"root{idx}")

        # --- NEW: save per-root raw JSON (unflattened) ---
        raw_fp = out_dir / f"{idx:02d}_{_slug(title)}_{ts}_raw.json"
        raw_fp.write_text(json.dumps(root, indent=2, ensure_ascii=False))
        print(f"  ├─ raw JSON → {raw_fp}")

        # Flatten to rows/columns
        df = _questions_df(root)

        # Localize/format timestamp-like numeric columns if present
        ts_cols = [c for c in df.columns if ("timeStamp" in c) and df[c].dtype != "object"]
        if ts_cols:
            df[ts_cols] = (
                pd.to_datetime(df[ts_cols].stack(), unit="ms", utc=True)
                  .dt.tz_convert(tz)
                  .dt.strftime("%Y-%m-%d %H:%M:%S")
                  .unstack()
            )

        # Save CSV per root
        fn = f"{idx:02d}_{_slug(title)}_{ts}_{len(df)}rows.csv"
        fp = out_dir / fn
        df.to_csv(fp, index=False)
        print(f"  └─ CSV ({len(df)} rows) → {fp}")

        dfs[title] = df

    return dfs


# ───────────────────────────────────────────── 5 | PUBLIC FUNCTION
def get_interactions(*,
                     connection_id: int | None = None,
                     user_code: str | None = None,
                     retries: int = 3,
                     out_base: Path | str = DEFAULT_BASE_OUT,
                     private_key_path: Path = DEFAULT_PRIVKEY_PATH
                    ) -> dict[str, pd.DataFrame]:
    """
    Retrieve and save interaction data for a single connection.

    Parameters:
        connection_id: Participant's connection ID.
        user_code: Practitioner code (5-character).
        retries: Retry count on API status –1.
        out_base: Output directory.
        privkey_path: Path to RSA private key.

    Returns:
        Dictionary mapping root container titles to DataFrames.
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

    print(f"Fetching interactions for connection {connection_id} …")
    roots = _fetch_interactions(user_code, connection_id, retries=retries, privkey_path=private_key_path)
    return _flatten_and_save_roots(roots, connection_id, out_dir)

# ───────────────────────────────────────────── 6 | CLI HANDLER
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download m-Path interactions.")
    parser.add_argument("--connection_id", type=int, help="Connection/participant ID")
    parser.add_argument("--user_code", type=str, help="5-char practitioner code (overrides env)")
    args, _ = parser.parse_known_args()

    try:
        get_interactions(connection_id=args.connection_id, user_code=args.user_code)
    except Exception as e:
        sys.exit(f"Failed: {e}")
