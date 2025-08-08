"""Auto-generated flat API for ema_api_handling."""
from importlib import import_module as _im

_EXPORTS = {
    "generate_keys": [
        "detect_rc_file",
        "read_rc",
        "write_rc",
        "set_user_code",
        "generate_keys",
    ],
    "get_clients": [
        "dataclass",
        "replace",
        "make_jwt",
        "normalize_changed_after",
        "get_clients",
        "resolve_user_code",
        "get_client_ids_and_aliases",
    ],
    "merge_and_push_schedule": [
        "build_entries",
        "merge_and_push",
    ],
    "get_schedule": [
        "get_schedule",
    ],
    "get_interactions": [
        "get_interactions",
    ],
    "get_data": [
        "make_jwt",
        "get_data",
        "flatten_rows",
        "flatten_and_save",
    ],
    "schedule_json_builder": [
        "combine_entries",
        "save_upload_json",
    ],
    "set_interactions_from_json": [
    "set_interactions",
    "set_interactions_from_json",  # so you can import and call this name directly
],

}

for _mod, _names in _EXPORTS.items():
    _m = _im(f"{__name__}.{_mod}")
    for _n in _names:
        globals()[_n] = getattr(_m, _n)

__all__ = [n for names in _EXPORTS.values() for n in names]
del _im, _mod, _names, _m, _n
