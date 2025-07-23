#!/usr/bin/env python3
# set_interactions_from_json.py – Upload m-Path interactions from local JSON
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

import requests, json, jwt, os, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ─────────────────────────────────────────────── 0 | CREDENTIAL SETUP
BASE_URL = "https://m-path.io/API2"
USER_CODE = os.getenv("MPATH_USERCODE")
if not USER_CODE:
    sys.exit("MPATH_USERCODE is not set (see generate_keys.py).")

if os.getenv("MPATH_CONNECTION_ID", "").isdigit():
    CONNECTION_ID = int(os.environ["MPATH_CONNECTION_ID"])
else:
    CONNECTION_ID = int(input("Enter numeric CONNECTION ID: ").strip())

KEY_PRIV = Path(os.getenv("MPATH_PRIVKEY", Path.home() / ".mpath_private_key.pem"))
if not KEY_PRIV.exists():
    sys.exit(f"RSA private key not found: {KEY_PRIV}")

# ─────────────────────────────────────────────── 1 | JWT GENERATOR
def make_jwt(ttl=5) -> str:
    """Generate a short-lived (ttl minutes) JWT for secure API access."""
    exp = datetime.now(timezone.utc) + timedelta(minutes=ttl)
    return jwt.encode({"exp": exp, "userCode": USER_CODE},
                      KEY_PRIV.read_text(), algorithm="RS256")

# ─────────────────────────────────────────────── 2 | INTERACTION UPLOADER
def set_interactions(interactions: list[dict]) -> None:
    """
    Upload a replacement interaction (questionnaire) list to m-Path.
    Sends the full JSON as a query string parameter (not as body).
    """
    payload = json.dumps(interactions, ensure_ascii=False)
    params = {
        "userCode": USER_CODE,
        "connectionId": CONNECTION_ID,
        "JWT": make_jwt(),
        "interactionsJSON": payload,
    }

    resp = requests.post(f"{BASE_URL}/setInteractions", params=params, timeout=30)
    resp.raise_for_status()

    try:
        body = resp.json()
    except ValueError:
        print("Raw response (non-JSON):\n", resp.text)
        return

    print("Server reply:\n", json.dumps(body, indent=2, ensure_ascii=False))

    if body.get("status") != 1:
        raise RuntimeError("API rejected the payload.")

# ─────────────────────────────────────────────── 3 | CLI TEST
if __name__ == "__main__":
    import pathlib

    json_file = pathlib.Path(
        "/Users/jimmy/github/leekh3-ema-processing/api_handling/example_json_input/interactions_test.json"
    )

    if not json_file.is_file():
        raise FileNotFoundError(f"{json_file} does not exist.")

    interactions = json.loads(json_file.read_text(encoding="utf-8"))
    print(f"Uploading {len(interactions)} interaction block(s)…")
    set_interactions(interactions)
