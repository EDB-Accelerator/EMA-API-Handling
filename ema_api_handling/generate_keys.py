#!/usr/bin/env python3
# generate_keys.py – one-time setup script for m-Path API credentials
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
generate_keys.py

Interactive script to configure m-Path credentials.

- Prompts user for their 5-character user code
- Persists it in the user's shell profile (e.g. ~/.bashrc, ~/.zshrc)
- Generates private/public RSA key pair if not already present
"""

from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone, timedelta
import os, subprocess, sys, json, jwt, stat

# ───────────────────────────────────────────── 0 | CONSTANTS
BASE_URL = "https://m-path.io/API2"
PRIVATE_KEY_PEM = Path.home() / ".mpath_private_key.pem"
PUBLIC_KEY_PEM  = Path.home() / ".mpath_public_key.pem"

# ───────────────────────────────────────────── 1 | SHELL RC HELPERS
def detect_rc_file() -> Path:
    """Return the user's shell startup file (e.g., .zshrc or .bashrc)."""
    shell = os.environ.get("SHELL", "")
    if "zsh" in shell:
        return Path.home() / ".zshrc"
    if "bash" in shell:
        return Path.home() / (".bash_profile" if sys.platform == "darwin" else ".bashrc")
    return Path.home() / ".profile"

def read_rc(rc_path: Path) -> list[str]:
    """Read the lines from the RC file if it exists."""
    if rc_path.exists():
        return rc_path.read_text(errors="ignore").splitlines()
    return []

def write_rc(rc_path: Path, lines: list[str]) -> None:
    """Write updated lines to the RC file and set secure permissions."""
    rc_path.write_text("\n".join(lines) + "\n")
    rc_path.chmod(rc_path.stat().st_mode & ~stat.S_IWGRP & ~stat.S_IWOTH)

# ───────────────────────────────────────────── 2 | USER CODE SETUP
def set_user_code() -> str:
    """
    Prompt for or reuse a 5-character m-Path user code.

    Persists it in the shell RC file and returns the value.
    """
    env_code = os.getenv("MPATH_USERCODE")
    rc_path = detect_rc_file()
    rc_lines = read_rc(rc_path)

    # Check if already defined in RC file
    existing_line_idx = next(
        (i for i, ln in enumerate(rc_lines)
         if ln.strip().startswith("export MPATH_USERCODE=")),
        None,
    )

    file_code = None
    if existing_line_idx is not None:
        file_code = rc_lines[existing_line_idx].split("=", 1)[1].strip('"\' \t')

    current_code = env_code or file_code
    if current_code:
        print(f"Current MPATH_USERCODE is “{current_code}”.")
        keep = input("Keep this value? [Y/n] ").strip().lower()
        if keep in ("", "y", "yes"):
            return current_code

    # Prompt for new code if needed
    while True:
        code = input("Enter your 5-character m-Path user code (e.g. abcd1): ").strip()
        if len(code) == 5 and code.isalnum():
            break
        print("Please enter exactly five letters/digits.")

    # Update or append export line
    export_line = f'export MPATH_USERCODE="{code}"'
    if existing_line_idx is not None:
        rc_lines[existing_line_idx] = export_line
    else:
        rc_lines.append(export_line)
    write_rc(rc_path, rc_lines)
    print(f"Added to {rc_path}. Open a new terminal or run 'source {rc_path}' to apply.")

    # Set for current process
    os.environ["MPATH_USERCODE"] = code
    return code

# ───────────────────────────────────────────── 3 | KEY GENERATION
def generate_keys() -> None:
    """
    Generate RSA private and public keys if they do not exist.
    """
    if PRIVATE_KEY_PEM.exists():
        print(f"Private key already present → {PRIVATE_KEY_PEM}")
        return

    print("Generating 2048-bit RSA key pair …")
    subprocess.run(
        ["openssl", "genpkey", "-algorithm", "RSA", "-out", str(PRIVATE_KEY_PEM),
         "-pkeyopt", "rsa_keygen_bits:2048"], check=True)
    subprocess.run(
        ["openssl", "rsa", "-in", str(PRIVATE_KEY_PEM),
         "-pubout", "-out", str(PUBLIC_KEY_PEM)], check=True)
    print(f"Keys written:\n  {PRIVATE_KEY_PEM}\n  {PUBLIC_KEY_PEM}")

# ───────────────────────────────────────────── 4 | MAIN
if __name__ == "__main__":
    print("────────────────────────────────────────────")
    print(" m-Path key setup and user code registration")
    print("────────────────────────────────────────────")

    user_code = set_user_code()
    print(f"Using MPATH_USERCODE = {user_code}")

    generate_keys()

    print("Setup complete.")
