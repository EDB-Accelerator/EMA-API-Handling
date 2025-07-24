# M-Path API Utilities

A small toolkit to download raw data, inspect interactions, manage schedules, and upload new beeps to the m-Path platform from Python or the command line.

## Features
- Download full connection datasets (`get_data.py`) and flatten them to CSV.
- Retrieve interaction logs (`get_interactions.py`) for quick QA.
- Pull an existing schedule (`get_schedule.py`).
- Build additional beep entries (`merge_and_push_schedule.py`).
- Combine and save entries to upload-ready JSON (`schedule_json_builder.py`).
- Upload schedules to m-Path via a secure, retry-safe client (`set_schedule_from_json.py`).

## Repository Structure
```

.
├── example.ipynb                 # End-to-end notebook demo
├── get\_data.py
├── get\_interactions.py
├── get\_schedule.py
├── merge\_and\_push\_schedule.py
├── schedule\_json\_builder.py
├── set\_schedule\_from\_json.py
└── README.md

````

## Requirements
- Python 3.9+
- `pandas`, `requests`, `pyjwt` (JWT), `ipywidgets` (optional for notebook UI)
- An RSA key pair placed at `~/.mpath_private_key.pem` and `~/.mpath_public_key.pem`

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
````

Set the following environment variables (for convenience):

```bash
export MPATH_USERCODE="ukmp2"
export MPATH_CONNECTION_ID="290982"
export MPATH_PRIVKEY="$HOME/.mpath_private_key.pem"
```

## Quick-Start Notebook

Open **[example.ipynb](./example.ipynb)** and run the cells top-to-bottom.
It demonstrates:

1. Downloading raw data.
2. Flattening to a clean DataFrame.
3. Pulling and filtering the existing schedule.
4. Creating new beeps.
5. Combining and saving to JSON.
6. Uploading the schedule via the API.

## Command-Line Upload

```bash
python set_schedule_from_json.py upload_ready_YYYYMMDDTHHMMSS.json \
  --user_code ukmp2 \
  --connection_id 290982 \
  --privkey ~/.mpath_private_key.pem \
  --minimal
```

## Troubleshooting

* **401 Unauthorized**: Verify `userCode`, `connectionId`, and that the JWT has not expired.
* **Status –1 responses**: The script automatically retries; increase `--retries` if needed.
* **Timezone issues**: All timestamps should be localised consistently (e.g., `US/Eastern`).

## Suggested `.gitignore`

```
# Keys and credentials
*.pem
.env

# Runtime files
mpath_raw/
upload_ready_*.json
__pycache__/
.venv/
```

## License

MIT License – see `LICENSE` for details.

## Contact

Kyunghun Lee
[kyunghun.lee@nih.gov](mailto:kyunghun.lee@nih.gov)
