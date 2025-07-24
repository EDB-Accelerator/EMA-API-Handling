# schedule_json_builder.py
from __future__ import annotations
from collections import OrderedDict, abc as _abc
from datetime import datetime as _dt
import ast, json, time, pathlib
import pandas as pd

# Public API -------------------------------------------------------------------
__all__ = [
    "combine_entries",
    "save_upload_json",
    "ORDER",
    "TIME_KEYS",
]

# Configuration ----------------------------------------------------------------
ORDER = [
    "itemId", "localId", "endTime", "startTime",
    "beepId", "scheduledTime", "reminderIntervals", "randomizationScheme"
]
TIME_KEYS = {"startTime", "endTime", "scheduledTime"}

# Helpers ----------------------------------------------------------------------
def _fmt_time(v):
    """Return 'YYYY-MM-DD HH:MM:SS' (strip T/offset)."""
    if pd.isna(v):
        return None
    if isinstance(v, (pd.Timestamp, _dt)):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v).replace("T", " ").split("+")[0]

def _fix_reminders(v):
    """Return list[int] or None."""
    if v is None or (isinstance(v, (list, tuple)) and len(v) == 0) or pd.isna(v):
        return None
    if isinstance(v, str):
        try:
            parsed = ast.literal_eval(v.strip())
            if isinstance(parsed, _abc.Sequence):
                return list(parsed)
        except Exception:
            return None
    if isinstance(v, _abc.Sequence):
        return list(v)
    return None

def _to_api_dict(src: dict | pd.Series, idx: int, is_new: bool) -> OrderedDict:
    """Build an OrderedDict in the desired key order for the m-Path API."""
    od = OrderedDict()
    for k in ORDER:
        if k == "beepId":
            # set to 0 for new rows, or keep old (here we always set 0 per your original code)
            od[k] = 0
        elif k == "localId":
            od[k] = src.get(k) or f"auto_{idx+1:04d}"    # generate if missing
        elif k == "reminderIntervals":
            ri = _fix_reminders(src.get(k))
            if ri:
                od[k] = ri
        elif k in src and pd.notna(src[k]):
            od[k] = _fmt_time(src[k]) if k in TIME_KEYS else src[k]
    return od

# Main functions ---------------------------------------------------------------
def combine_entries(df_future: pd.DataFrame,
                    new_beeps: list[dict],
                    order: list[str] = ORDER,
                    time_keys: set[str] = TIME_KEYS) -> list[OrderedDict]:
    """
    Combine existing future entries (DataFrame rows) and newly built entries (dicts)
    into a single list of OrderedDicts ready for m-Path JSON upload.
    """
    global ORDER, TIME_KEYS
    ORDER = order
    TIME_KEYS = time_keys

    future_entries = [
        _to_api_dict(row, i, is_new=False)
        for i, (_, row) in enumerate(df_future.iterrows())
    ]
    offset = len(future_entries)

    new_entries = [
        _to_api_dict(e, i + offset, is_new=True)
        for i, e in enumerate(new_beeps)
    ]

    combined = future_entries + new_entries
    return combined

def save_upload_json(combined: list[OrderedDict],
                     filename_prefix: str = "upload_ready_") -> pathlib.Path:
    """
    Save the combined list as JSON with a timestamp-based filename.
    Returns the full Path to the saved file.
    """
    ts = time.strftime("%Y%m%dT%H%M%S")
    json_path = pathlib.Path(f"{filename_prefix}{ts}.json").resolve()
    json_path.write_text(json.dumps(combined, ensure_ascii=False, indent=2))
    return json_path
