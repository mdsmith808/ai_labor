#!/usr/bin/env python3
"""
build_cps_occ_to_soc_v2.py
Create CPS OCC -> SOC crosswalk CSV.

Usage (recommended online):
  python src/python/build_cps_occ_to_soc_v2.py --out data/cps_occ_to_soc.csv

Usage (offline/local file):
  python src/python/build_cps_occ_to_soc_v2.py --src data/2018_crosswalk.xlsx --out data/cps_occ_to_soc.csv

Options:
  --no-verify   Last-resort flag to bypass SSL verification (not recommended).

Requirements:
  pip install pandas requests openpyxl certifi
"""
import argparse, io, re, sys, os
import requests
import pandas as pd
import certifi

URL = "https://www2.census.gov/programs-surveys/demo/guidance/industry-occupation/2018-occupation-code-list-and-crosswalk.xlsx"

def find_cols(df):
    occ_candidates = [c for c in df.columns if re.search(r"(2018).*census.*(code)", c, flags=re.I)]
    soc_candidates = [c for c in df.columns if re.search(r"(2018).*soc.*(code)", c, flags=re.I)]
    if not occ_candidates:
        occ_candidates = [c for c in df.columns if re.search(r"\bcensus\b.*code", c, flags=re.I)]
    if not soc_candidates:
        soc_candidates = [c for c in df.columns if re.search(r"\bsoc\b.*code", c, flags=re.I)]
    if not occ_candidates or not soc_candidates:
        raise RuntimeError(f"Could not locate needed columns. Columns: {list(df.columns)}")
    return occ_candidates[0], soc_candidates[0]

def clean_soc(val):
    if pd.isna(val): return None
    s = str(val).strip().split('.')[0]
    if re.fullmatch(r"\d{2}\d{4}", s):
        s = s[:2] + "-" + s[2:]
    return s

def clean_occ(val):
    if pd.isna(val): return None
    try:
        i = int(float(val)); return f"{i:04d}"
    except Exception:
        import re
        s = re.sub(r"\D", "", str(val))
        return f"{int(s):04d}" if s else None

def build_from_excel(xls_bytes: bytes) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(xls_bytes), engine="openpyxl")
    df = None
    for sheet in xls.sheet_names:
        tmp = xls.parse(sheet)
        try:
            occ_col, soc_col = find_cols(tmp)
            df = tmp[[occ_col, soc_col]].copy()
            break
        except Exception:
            continue
    if df is None:
        raise RuntimeError("No sheet with the needed columns found.")
    df["occ"] = df.iloc[:,0].map(clean_occ)
    df["soc"] = df.iloc[:,1].map(clean_soc)
    df = df.dropna(subset=["occ","soc"]).drop_duplicates(subset=["occ"], keep="first")
    return df[["occ","soc"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output CSV (e.g., data/cps_occ_to_soc.csv)")
    ap.add_argument("--src", help="Local Excel file path (if you downloaded manually)")
    ap.add_argument("--no-verify", action="store_true", help="Bypass SSL verification (NOT recommended)")
    args = ap.parse_args()

    if args.src:
        with open(args.src, "rb") as f:
            xls_bytes = f.read()
    else:
        verify = False if args.no_verify else certifi.where()
        # allow override via env SSL_CERT_FILE if user set it
        if os.environ.get("SSL_CERT_FILE"):
            verify = os.environ["SSL_CERT_FILE"]
        print("[crosswalk] downloading from Census…")
        r = requests.get(URL, timeout=180, verify=verify)
        r.raise_for_status()
        xls_bytes = r.content

    print("[crosswalk] parsing workbook…")
    df = build_from_excel(xls_bytes)

    out = args.out
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_csv(out, index=False)
    print(f"[crosswalk] wrote {out} with {len(df)} rows.")

if __name__ == "__main__":
    main()
