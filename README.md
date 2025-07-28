
# M-Path API Utilities

A small toolkit to download raw data, inspect interactions, manage schedules, and upload new beeps to the m-Path platform from Python or the command line.

## Features
- Download client metadata (`get_clients.py`) and save raw JSON per connection.
- Download full connection datasets (`get_data.py`) and flatten them to CSV.
- Retrieve interaction trees (`get_interactions.py`) and export each root container to CSV.
- Pull an existing schedule (`get_schedule.py`) and flatten to CSV.
- Build additional beep entries and push them safely (`merge_and_push_schedule.py`).
- Combine and save entries to upload‑ready JSON (`schedule_json_builder.py`).
- Upload schedules to m-Path via a secure, retry-safe client (`set_schedule_from_json.py`).

All scripts accept **optional parameters** for:
- RSA private key path (`privkey_path`)
- Base output directory (`out_base` / `base_dump_dir`)
- User code (`user_code`)
- Connection ID (`connection_id`)

Defaults fall back to environment variables and sensible paths.

## Repository Structure

```

.
├── example.ipynb                     # End-to-end notebook demo
├── get\_clients.py
├── get\_data.py
├── get\_interactions.py
├── get\_schedule.py
├── merge\_and\_push\_schedule.py
├── schedule\_json\_builder.py
├── set\_schedule\_from\_json.py
└── README.md

```

## Requirements
- Python 3.9+
- `pandas`, `requests`, `pyjwt`
- `ipywidgets` (optional, for notebook UI)

You also need an RSA key pair:
```

\~/.mpath\_private\_key.pem
\~/.mpath\_public\_key.pem

````

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
````

Set convenient environment variables (optional but recommended):

```bash
export MPATH_USERCODE="ukmp2"
export MPATH_CONNECTION_ID="290982"
export MPATH_PRIVKEY="$HOME/.mpath_private_key.pem"
```

## Script Overview

| Script                       | Purpose                                         | Output                    | Key Args (override defaults)                        |
| ---------------------------- | ----------------------------------------------- | ------------------------- | --------------------------------------------------- |
| `get_clients.py`             | Download raw client metadata (no flattening)    | JSON files per connection | `privkey_path`, `base_dump_dir`, `user_code`        |
| `get_data.py`                | Download raw data and flatten to CSV            | Raw JSON + flattened CSV  | `private_key_path`, `base_dump_dir`, `user_code`    |
| `get_interactions.py`        | Fetch & flatten interaction trees per root      | Raw JSON + CSV per root   | `privkey_path`, `out_base`, `user_code`             |
| `get_schedule.py`            | Fetch & flatten schedule rows                   | Raw JSON + one CSV        | `privkey_path`, `out_base`, `user_code`             |
| `merge_and_push_schedule.py` | Merge new beeps and push schedule back          | API response JSON         | `privkey_path`, `user_code`                         |
| `schedule_json_builder.py`   | Build upload-ready schedule JSON from DataFrame | JSON                      | N/A (pure local builder)                            |
| `set_schedule_from_json.py`  | Upload a JSON schedule file to m-Path           | API response JSON         | `privkey_path`, `user_code`, `connection_id`, flags |

## Quick-Start Notebook

Open **[example.ipynb](./example.ipynb)** and run cells top-to-bottom. It demonstrates:

1. Downloading client metadata.
2. Downloading raw data and flattening to a clean DataFrame.
3. Pulling and filtering the existing schedule.
4. Creating new beeps and merging with the current schedule.
5. Saving upload-ready JSON.
6. Uploading via the API.

## Common Usage Patterns

### Jupyter / Python API

```python
from pathlib import Path
from get_clients import get_clients
from get_data import get_data
from get_schedule import get_schedule
from merge_and_push_schedule import build_entries, merge_and_push

# Clients
clients, out_dir = get_clients(
    user_code="ukmp2",
    base_dump_dir=Path("./clients_raw"),
    private_key_path=Path("/keys/mpath_priv.pem")
)

# Data
rows, data_dir = get_data(
    user_code="ukmp2",
    connection_id=290982,
    base_dump_dir=Path("./mpath_data"),
    private_key_path=Path("/keys/mpath_priv.pem")
)

# Schedule
df_sched = get_schedule(
    connection_id=290982,
    user_code="ukmp2",
    out_base=Path("./schedule_raw"),
    privkey_path=Path("/keys/mpath_priv.pem")
)

# Merge & push
new_entries = build_entries(
    starts=["2025-08-01 09:00:00"],
    ends=["2025-08-01 09:15:00"],
    item_id="N1m7ygNkbTTi6N8D",
    labels=["aug01_beep"]
)
merge_and_push(
    connection_id=290982,
    new_entries=new_entries,
    user_code="ukmp2",
    privkey_path=Path("/keys/mpath_priv.pem")
)
```

### Command Line Examples

```bash
# Clients
python get_clients.py --user_code ukmp2 --connection_id 290982 \
  --privkey ~/.mpath_private_key.pem --out ./clients_raw

# Data
python get_data.py --user_code ukmp2 --connection_id 290982 \
  --privkey ~/.mpath_private_key.pem --out ./mpath_data

# Schedule
python get_schedule.py --user_code ukmp2 --connection_id 290982 \
  --privkey ~/.mpath_private_key.pem --out ./schedule_raw

# Upload a prepared JSON
python set_schedule_from_json.py upload_ready_20250728T130000.json \
  --user_code ukmp2 --connection_id 290982 --privkey ~/.mpath_private_key.pem --minimal
```

> Each script’s `--help` will show all flags.

## Troubleshooting

* **401 Unauthorized**: Check `user_code`, `connection_id`, JWT expiry, and private key path.
* **Status –1 responses**: The scripts retry automatically; increase `retries` if needed.
* **Timezone issues**: All timestamps are converted with a configurable timezone (default `US/Eastern`). Confirm your expected local zone.

## License

MIT License – see `LICENSE` for details.

## Contact

Kyunghun Lee
[kyunghun.lee@nih.gov](mailto:kyunghun.lee@nih.gov)
