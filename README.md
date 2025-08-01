# M‑Path API Utilities

A small toolkit for downloading raw EMA data, inspecting interactions, managing schedules and uploading new beeps to the **m‑Path** platform — from **Python** *or* the command‑line.

---

## Features

| Capability                                | Script / Function                                       | Output                       |
| ----------------------------------------- | ------------------------------------------------------- | ---------------------------- |
| Download client metadata                  | `list_clients()` · **CLI:** `get-clients`               | one JSON file per connection |
| Download full connection datasets         | `download_flattened_csv()` · **CLI:** `get-data`        | raw JSON + flattened CSV     |
| Retrieve interaction trees                | `get_interactions()` · **CLI:** `get-interactions`      | raw JSON + one CSV per root  |
| Pull existing schedule                    | `get_schedule()` · **CLI:** `get-schedule`              | raw JSON + single CSV        |
| Build extra beep entries                  | `build_entries()`                                       | in‑memory `DataFrame`        |
| Merge & push schedule                     | `merge_and_push_schedule()` · **CLI:** `merge-and-push` | API response JSON            |
| Combine + save beeps to upload‑ready JSON | `save_upload_json()`                                    | JSON file                    |
| Upload a schedule JSON                    | `set_schedule_from_json()` · **CLI:** `set-schedule`    | API response JSON            |

All functions accept optional overrides for:

* **`privkey_path`** – RSA private key (default `$HOME/.mpath_private_key.pem`)
* **`user_code`** – short m‑Path user code
* **`connection_id`** – numeric connection ID
* **`out_base` / `base_dump_dir`** – root output folder

Defaults fall back to environment variables and sensible paths.

---

## Requirements

* Python ≥ 3.9
* `pandas`, `requests`, `pyjwt`
* `ipywidgets` *(optional – notebook UI)*

You’ll also need an RSA key pair accepted by your m‑Path instance:

```text
~/.mpath_private_key.pem
~/.mpath_public_key.pem
```

---

## Installation

### 1 · From GitHub (recommended)

```bash
# Using SSH (no password prompt if you have keys set up)
pip install git+ssh://git@github.com/EDB-Accelerator/EMA-API-Handling.git@package-refactor

# …or via HTTPS
git clone --branch package-refactor https://github.com/EDB-Accelerator/EMA-API-Handling.git
cd EMA-API-Handling
pip install -e .    # editable / developer install
```

### 2 · From a local path

```bash
pip install -e /path/to/EMA-API-Handling
```

> The editable (`-e`) install lets you hack on the code and pick up changes without reinstalling.

Set convenient environment variables *(optional but handy)*:

```bash
export MPATH_USERCODE="ukmp2"
export MPATH_CONNECTION_ID="290982"
export MPATH_PRIVKEY="$HOME/.mpath_private_key.pem"
```

---

## Package‑level API

After installation everything is exposed at the package root:

```python
import ema_api_handling as ema

# Download + flatten raw data
df = ema.download_flattened_csv(
        user_code="ukmp2",
        connection_id=290982,
        privkey_path="~/.mpath_private_key.pem",
        tz="US/Eastern"
)

# Pull schedule
df_sched = ema.get_schedule(connection_id=290982)

# Build + push one new beep
new = ema.build_entries(
        starts=["2025‑08‑01 09:00"],
        ends=[  "2025‑08‑01 09:15"],
        item_id="N1m7ygNkbTTi6N8D",
        labels=["aug01_beep"]
)
ema.merge_and_push_schedule(connection_id=290982, new_entries=new)
```

Use tab‑completion (`ema.<TAB>`) to discover all functions.

---

## Command‑line usage

Every major task is also available as a console script once the package is installed:

```bash
# Show help for any command
get-data --help

# Clients
get-clients   --user_code ukmp2 --connection_id 290982 \
             --privkey ~/.mpath_private_key.pem   --out ./clients_raw

# Data
get-data      --user_code ukmp2 --connection_id 290982 \
             --privkey ~/.mpath_private_key.pem   --out ./mpath_data

# Schedule
get-schedule  --user_code ukmp2 --connection_id 290982 \
             --privkey ~/.mpath_private_key.pem   --out ./schedule_raw

# Upload prepared JSON
set-schedule  upload_ready_20250801T0900.json \
             --user_code ukmp2 --connection_id 290982 --minimal
```

---

## Repository layout (branch `package-refactor`)

```text
ema_api_handling/               # installable package
├── __init__.py                 # flat API re‑exports
├── get_data.py                 # ⇢ download_flattened_csv(), …
├── get_clients.py              # ⇢ list_clients()
├── get_schedule.py             # ⇢ get_schedule()
├── get_interactions.py         # ⇢ get_interactions()
├── merge_and_push_schedule.py  # ⇢ merge_and_push_schedule()
├── schedule_json_builder.py    # ⇢ save_upload_json()
├── set_schedule_from_json.py   # ⇢ set_schedule_from_json()
└── generate_keys.py            # key‑pair helpers
example.ipynb                   # end‑to‑end demo
pyproject.toml                  # build metadata
README.md
```

---

## Troubleshooting tips

| Symptom                   | Likely cause & fix                                                                        |
| ------------------------- | ----------------------------------------------------------------------------------------- |
| **401 Unauthorized**      | Wrong `user_code`, expired JWT, or wrong private key. Double‑check all three.             |
| **Status –1** from m‑Path | Temporary network hiccup; scripts retry automatically. Increase `retries` if needed.      |
| **Timezone confusion**    | All timestamps default to `US/Eastern`. Pass `tz="<Olson ID>"` or convert after download. |

---

## License

MIT – see `LICENSE` for full text.

---

## Contact

Kyunghun Lee  ·  [kyunghun.lee@nih.gov](mailto:kyunghun.lee@nih.gov)
