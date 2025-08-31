#!/usr/bin/env python3
"""
build_cps_occ_to_soc.py
Downloads the official *2018 Census Occupation Code List with Crosswalk* (Excel)
and writes a clean CSV crosswalk for CPS: columns = occ, soc.

Usage (from your ai_labor project root):
  python src/python/build_cps_occ_to_soc.py --out data/cps_occ_to_soc.csv

Requirements:
  pip install pandas requests openpyxl

Source:
- Census "2018 Census Occupation Code List with Crosswalk" (Excel)
  https://www2.census.gov/programs-surveys/demo/guidance/industry-occupation/2018-occupation-code-list-and-crosswalk.xlsx
"""

import argparse, io, re, sys
import requests
import pandas as pd

URL = "https://www2.census.gov/programs-surveys/demo/guidance/industry-occupation/2018-occupation-code-list-and-crosswalk.xlsx"

def find_cols(df):
    # Flexible match for columns
    cols = {c.lower(): c for c in df.columns}
    # heuristics for occ and soc columns
    occ_candidates = [c for c in df.columns if re.search(r"(2018).*census.*(code)", c, flags=re.I)]
    soc_candidates = [c for c in df.columns if re.search(r"(2018).*soc.*(code)", c, flags=re.I)]
    # fallback: shorter names
    if not occ_candidates:
        occ_candidates = [c for c in df.columns if re.search(r"\bcensus\b.*code", c, flags=re.I)]
    if not soc_candidates:
        soc_candidates = [c for c in df.columns if re.search(r"\bsoc\b.*code", c, flags=re.I)]
    if not occ_candidates or not soc_candidates:
        raise RuntimeError(f"Could not locate needed columns. Columns found: {list(df.columns)}")
    return occ_candidates[0], soc_candidates[0]

def clean_soc(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    # keep only patterns like 11-1021 or 11-1021.00, strip decimals
    s = s.split('.')[0]
    # add dash if missing and length looks like 6
    if re.fullmatch(r"\d{2}\d{4}", s):
        s = s[:2] + "-" + s[2:]
    return s

def clean_occ(val):
    if pd.isna(val):
        return None
    # Some sheets store as number; coerce to int then pad 4
    try:
        i = int(float(val))
        return f"{i:04d}"
    except Exception:
        s = re.sub(r"\D", "", str(val))
        if not s:
            return None
        return f"{int(s):04d}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Path to write CSV (e.g., data/cps_occ_to_soc.csv)")
    args = ap.parse_args()

    print("[crosswalk] downloading Excel from Census…")
    r = requests.get(URL, timeout=120)
    r.raise_for_status()

    print("[crosswalk] reading workbook…")
    # Try first sheet; if it doesn't work, try all until columns are found
    xls = pd.ExcelFile(io.BytesIO(r.content), engine="openpyxl")
    df = None
    for sheet in xls.sheet_names:
        try:
            tmp = xls.parse(sheet)
            occ_col, soc_col = find_cols(tmp)
            df = tmp[[occ_col, soc_col]].copy()
            break
        except Exception:
            continue
    if df is None:
        raise RuntimeError("Could not find a sheet with the needed columns.")

    print(f"[crosswalk] using columns: {occ_col!r} (occ), {soc_col!r} (soc)")
    df["occ"] = df[occ_col].map(clean_occ)
    df["soc"] = df[soc_col].map(clean_soc)
    df = df.dropna(subset=["occ", "soc"]).drop_duplicates(subset=["occ"], keep="first")

    out = args.out
    pd.options.display.max_columns = 20
    df[["occ","soc"]].to_csv(out, index=False)
    print(f"[crosswalk] wrote {out} with {len(df)} rows.")

if __name__ == "__main__":
    main()
