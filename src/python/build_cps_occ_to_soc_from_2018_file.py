#!/usr/bin/env python3
"""
build_cps_occ_to_soc_from_2018file.py
Reads the official "2018-occupation-code-list-and-crosswalk.xlsx"
and writes a CPS crosswalk CSV with columns: occ (4-digit), soc (xx-xxxx).

Usage:
  python build_cps_occ_to_soc_from_2018file.py \
      --xlsx data/2018_crosswalk.xlsx \
      --out  data/cps_occ_to_soc.csv
"""
import argparse, re
from pathlib import Path
import pandas as pd

def build(xlsx_path: str) -> pd.DataFrame:
    # The table lives on sheet "2018 Census Occ Code List" with headers at row 4.
    tbl = pd.read_excel(xlsx_path, sheet_name="2018 Census Occ Code List", header=4, engine="openpyxl")
    # We only need these two columns
    need = ["2018 Census Code", "2018 SOC Code"]
    missing = [c for c in need if c not in tbl.columns]
    if missing:
        raise SystemExit(f"Missing expected columns: {missing}")
    t = tbl[need].copy()
    # Extract 4-digit occ and 2-2-4 SOC
    t['occ'] = t['2018 Census Code'].astype(str).str.extract(r'(\\d{4})')
    t['soc'] = t['2018 SOC Code'].astype(str).str.extract(r'(\\d{2}-\\d{4})')
    t = t.dropna(subset=['occ','soc']).drop_duplicates()
    # Prefer specific SOC over '-0000' major group
    t['score'] = (t['soc'].str.endswith('0000')).map(lambda x: 0 if not x else 0)  # will override below
    # Assign score 1 for non-major group, 0 for '-0000'
    t['score'] = (~t['soc'].str.endswith('0000')).astype(int)
    t['row'] = range(len(t))
    t = t.sort_values(['occ','score','row'], ascending=[True, False, True])
    t = t.drop_duplicates('occ', keep='first')
    return t[['occ','soc']].reset_index(drop=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", required=True, help="Path to 2018-occupation-code-list-and-crosswalk.xlsx")
    ap.add_argument("--out", required=True, help="Output CSV path (e.g., data/cps_occ_to_soc.csv)")
    args = ap.parse_args()
    df = build(args.xlsx)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with {len(df)} rows.")

if __name__ == "__main__":
    main()
