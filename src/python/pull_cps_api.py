#!/usr/bin/env python3
"""
pull_cps_api.py  —  Create & download an IPUMS CPS extract via the API (v2)

Usage (ASEC 2024 example):
  python src/python/pull_cps_api.py \
    --samples cps2024_03s \
    --vars OCC ASECWT STATEFIP AGE SEX \
    --out data/cps_asec_2024.csv

Key points
- Reads API key from .env (supports IPUMS_API_KEY or IPUMS_KEY).
- Creates an extract for the given CPS samples with selected variables.
- Polls until the extract is "completed".
- Downloads the data (CSV or gzipped CSV) and writes to --out.
- Also writes a small JSON sidecar with the extract metadata (same path + ".meta.json").

Requirements
  pip install requests python-dotenv
"""
import argparse
import gzip
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, Any

import requests
from dotenv import load_dotenv, find_dotenv

API_BASE = "https://api.ipums.org"
COLLECTION = "cps"
API_VERSION = "2"   # IPUMS API v2

def _load_env():
    # Load from current working directory first if present
    load_dotenv(find_dotenv(usecwd=True), override=False)
    # Also try relative to this file (in case run from subdir)
    here = Path(__file__).resolve()
    for up in [here.parent, here.parent.parent, here.parent.parent.parent]:
        envp = up / ".env"
        if envp.exists():
            load_dotenv(envp.as_posix(), override=False)

def _ipums_key() -> str:
    return os.getenv("IPUMS_API_KEY") or os.getenv("IPUMS_KEY") or ""

def _headers() -> Dict[str, str]:
    key = _ipums_key()
    if not key:
        raise SystemExit("Missing IPUMS_API_KEY (or IPUMS_KEY). Put it in your .env at the project root.")
    return {"Authorization": key, "Content-Type": "application/json"}

def submit_extract(samples, variables, description="CPS extract (API v2)", data_format="csv") -> int:
    payload: Dict[str, Any] = {
        "description": description,
        "dataFormat": data_format,                 # "csv" or "fixed_width"
        "dataStructure": {"rectangular": {"on": "P"}},  # person records
        "samples": {s: {} for s in samples},
        "variables": {v: {} for v in variables},
    }
    url = f"{API_BASE}/extracts?collection={COLLECTION}&version={API_VERSION}"
    r = requests.post(url, headers=_headers(), data=json.dumps(payload), timeout=180)
    r.raise_for_status()
    resp = r.json()
    number = resp.get("number") or resp.get("id")
    if number is None:
        raise RuntimeError(f"Unexpected submit response: {resp}")
    return int(number)

def get_status(number: int):
    url = f"{API_BASE}/extracts/{number}?collection={COLLECTION}&version={API_VERSION}"
    r = requests.get(url, headers=_headers(), timeout=90)
    r.raise_for_status()
    return r.json()

def list_downloads(number: int):
    url = f"{API_BASE}/extracts/{number}/downloads?collection={COLLECTION}&version={API_VERSION}"
    r = requests.get(url, headers=_headers(), timeout=90)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()

def stream_download(url: str, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, headers={"Authorization": _ipums_key()}, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)
    return out_path

def maybe_gunzip(path_in: Path, dest_csv: Path) -> Path:
    if path_in.suffix == ".gz":
        import gzip
        with gzip.open(path_in, "rb") as gz, open(dest_csv, "wb") as out:
            shutil.copyfileobj(gz, out)
        path_in.unlink()
        return dest_csv
    return path_in

def main():
    _load_env()
    if not _ipums_key():
        raise SystemExit("Missing IPUMS_API_KEY (or IPUMS_KEY). Create .env with your key in the project root.")

    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", nargs="+", required=True, help="CPS sample ids, e.g., cps2024_03s cps2023_03s")
    ap.add_argument("--vars", nargs="+", required=True, help="Variables, e.g., OCC ASECWT STATEFIP AGE SEX")
    ap.add_argument("--out", required=True, help="Output CSV path, e.g., data/cps_asec_2024.csv")
    ap.add_argument("--desc", default="CPS extract (API v2)", help="Optional description for the extract")
    ap.add_argument("--format", default="csv", choices=["csv", "fixed_width"], help="Requested data format")
    ap.add_argument("--max-wait", type=int, default=60*30, help="Max seconds to wait (default 30m)")
    ap.add_argument("--poll-every", type=int, default=8, help="Seconds between polls (default 8)")
    args = ap.parse_args()

    out_csv = Path(args.out).resolve()
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    print(f"[ipums] submitting extract for samples={args.samples} vars={args.vars}")
    number = submit_extract(args.samples, args.vars, description=args.desc, data_format=args.format)
    print(f"[ipums] extract number: {number}")

    start = time.time()
    status = "queued"
    info = {}
    while True:
        if time.time() - start > args.max_wait:
            raise SystemExit(f"[ipums] timeout after {args.max_wait}s; last status={status}")
        time.sleep(args.poll_every)
        try:
            info = get_status(number)
            status = info.get("status", status)
            print(f"[ipums] status={status}")
            if status in ("completed", "failed", "canceled"):
                break
        except requests.HTTPError as e:
            print(f"[ipums] status check error: {e}; retrying...")

    if status != "completed":
        raise SystemExit(f"[ipums] extract ended with status={status}")

    data_url = None
    dl = info.get("downloadLinks") or {}
    if isinstance(dl, dict):
        data_block = dl.get("data") or dl.get("dataFile") or {}
        data_url = data_block.get("url")

    if not data_url:
        dl_resp = list_downloads(number)
        if isinstance(dl_resp, dict):
            links = dl_resp.get("links") or []
            for link in links:
                if isinstance(link, dict) and link.get("type") in ("data", "dataFile", "microdata", "csv"):
                    data_url = link.get("url")
                    if data_url:
                        break

    if not data_url:
        raise SystemExit("[ipums] could not locate a data download URL in API response.")

    tmp_path = out_csv
    if data_url.endswith(".gz") and not out_csv.suffix == ".gz":
        tmp_path = out_csv.with_suffix(out_csv.suffix + ".gz")

    print(f"[ipums] downloading -> {tmp_path}")
    stream_download(data_url, tmp_path)
    final_path = maybe_gunzip(tmp_path, out_csv)
    print(f"[ipums] done -> {final_path}")

    meta = {
        "extract_number": number,
        "samples": args.samples,
        "variables": args.vars,
        "status": status,
        "requested_format": args.format,
        "description": args.desc,
        "download_url": data_url,
        "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    meta_path = Path(str(final_path) + ".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2))
    print(f"[ipums] wrote metadata -> {meta_path}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[ipums] interrupted by user."); sys.exit(130)
    except SystemExit as e:
        print(e); sys.exit(getattr(e, "code", 1))
    except Exception as e:
        print(f"[ipums] fatal error: {e}"); sys.exit(1)
