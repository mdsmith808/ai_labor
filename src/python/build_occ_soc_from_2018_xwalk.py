#!/usr/bin/env python3
"""
Build a canonical OCC→SOC map from the 2018 Census occupation workbook.

Inputs
------
--xlsx : path to '2018_crosswalk.xlsx' (the Census workbook that contains SOC)
--out  : path to write 'occ,soc' CSV (e.g., data/cps_occ_to_soc.csv)
--sheet (optional): force a specific sheet name

Behavior
--------
- If --sheet is not given, scans all sheets and picks the first one that
  clearly contains BOTH:
    * an OCC-like column (mostly 3–4 digit numbers)
    * a SOC-like column (either 6 digits or 'NN-NNNN')
- Cleans:
    occ -> rightmost 4 digits, zero-padded (e.g., '10' -> '0010')
    soc -> first 6 digits -> 'NN-NNNN'
- Drops malformed rows, de-duplicates, and enforces one row per OCC.

Example
-------
python src/python/build_occ_soc_from_2018_xwalk.py \
  --xlsx data/2018_crosswalk.xlsx \
  --out  data/cps_occ_to_soc.csv
"""

import argparse
import re
import sys
from pathlib import Path
import pandas as pd

# Patterns to detect column CONTENT (not headers)
OCC_VAL = re.compile(r'^\s*\d{3,4}\s*$')                  # 3–4 digits (we'll zfill to 4)
SOC_VAL1 = re.compile(r'^\s*\d{6}\s*$')                   # 6 digits
SOC_VAL2 = re.compile(r'^\s*\d{2}\D+\d{4}\s*$')           # e.g., 11-1011

def score_columns(df: pd.DataFrame):
    """
    For each column, compute:
      - occ_score: share of cells that look like 3–4 digits
      - soc_score: share of cells that look like SOC (6 digits or NN-NNNN)
    Return best occ col, best soc col, and their scores.
    """
    best_occ, best_occ_sc = None, -1.0
    best_soc, best_soc_sc = None, -1.0

    for c in df.columns:
        s = df[c].astype(str)
        tot = (s != "nan").sum()
        if tot == 0:
            continue
        occ_sc = (s.str.match(OCC_VAL, na=False)).sum() / tot
        soc_sc = ((s.str.match(SOC_VAL1, na=False) | s.str.match(SOC_VAL2, na=False))).sum() / tot

        # Light name hints (don’t rely on them)
        cname = str(c).lower()
        if any(k in cname for k in ("occ", "2018", "census")):
            occ_sc += 0.02
        if "soc" in cname:
            soc_sc += 0.02

        if occ_sc > best_occ_sc:
            best_occ, best_occ_sc = c, occ_sc
        if soc_sc > best_soc_sc:
            best_soc, best_soc_sc = c, soc_sc

    # Ensure they aren't the same column; if so, pick second-best SOC
    if best_occ == best_soc:
        second_soc, second_sc = None, -1.0
        for c in df.columns:
            if c == best_occ:
                continue
            s = df[c].astype(str)
            tot = (s != "nan").sum()
            if tot == 0:
                continue
            sc = ((s.str.match(SOC_VAL1, na=False) | s.str.match(SOC_VAL2, na=False))).sum() / tot
            if "soc" in str(c).lower():
                sc += 0.02
            if sc > second_sc:
                second_soc, second_sc = c, sc
        if second_soc is not None:
            best_soc, best_soc_sc = second_soc, second_sc

    return best_occ, best_occ_sc, best_soc, best_soc_sc

def clean_map(df: pd.DataFrame, occ_col: str, soc_col: str) -> pd.DataFrame:
    """
    Produce a two-column, canonical map: occ (4-digit), soc (NN-NNNN)
    """
    s_occ = df[occ_col].astype(str).str.strip()
    s_soc = df[soc_col].astype(str).str.strip()

    # OCC: digits only, rightmost 4, zfill
    occ = s_occ.str.replace(r'\D', '', regex=True).str[-4:].str.zfill(4)

    # SOC: digits only, first 6 → NN-NNNN
    soc6 = s_soc.str.replace(r'\D', '', regex=True).str[:6]
    soc = soc6.str[:2] + "-" + soc6.str[2:]

    out = pd.DataFrame({"occ": occ, "soc": soc})
    out = out[out["occ"].str.match(r'^\d{4}$', na=False)]
    out = out[out["soc"].str.match(r'^\d{2}-\d{4}$', na=False)]
    out = out.drop_duplicates(subset=["occ", "soc"]).sort_values(["occ", "soc"])
    # Enforce one SOC per OCC (keep first appearance)
    out = out.drop_duplicates(subset=["occ"], keep="first")
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", required=True, help="path to 2018_crosswalk.xlsx")
    ap.add_argument("--out", required=True, help="path to write occ,soc CSV")
    ap.add_argument("--sheet", default=None, help="force exact sheet name (optional)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    xlsx = Path(args.xlsx)
    if not xlsx.exists():
        print(f"[error] no such file: {xlsx}", file=sys.stderr)
        sys.exit(1)

    xls = pd.ExcelFile(xlsx, engine="openpyxl")
    sheets = xls.sheet_names
    print(f"[sheets] {sheets}")

    chosen_df = None
    chosen_sheet = None
    chosen_occ = chosen_soc = None
    chosen_scores = (None, None)

    # If user forced a sheet, use it; else search
    sheet_iter = [args.sheet] if args.sheet else sheets
    for s in sheet_iter:
        try:
            df = pd.read_excel(xlsx, sheet_name=s, dtype=str, engine="openpyxl")
        except Exception as e:
            print(f"[skip] sheet='{s}': read error: {e}", file=sys.stderr)
            continue

        occ_col, occ_sc, soc_col, soc_sc = score_columns(df)
        if args.verbose:
            print(f"[scan] sheet='{s}' occ='{occ_col}'({occ_sc:.3f}) soc='{soc_col}'({soc_sc:.3f})")

        # Heuristic: require reasonable signals for BOTH and distinct cols
        if occ_col and soc_col and occ_col != soc_col and occ_sc >= 0.50 and soc_sc >= 0.50:
            chosen_df = df
            chosen_sheet = s
            chosen_occ, chosen_soc = occ_col, soc_col
            chosen_scores = (occ_sc, soc_sc)
            break

    if chosen_df is None:
        print("[error] could not find a sheet with both OCC and SOC columns.", file=sys.stderr)
        print("        Try --sheet with the exact name that contains SOC codes.", file=sys.stderr)
        sys.exit(2)

    print(f"[choose] sheet='{chosen_sheet}' occ_col='{chosen_occ}' soc_col='{chosen_soc}' "
          f"occ_score={chosen_scores[0]:.3f} soc_score={chosen_scores[1]:.3f}")

    out = clean_map(chosen_df, chosen_occ, chosen_soc)
    print(f"[build] rows={len(out)}")

    if len(out) == 0:
        print("[error] mapping cleaned to 0 rows; double-check the sheet choice.", file=sys.stderr)
        sys.exit(3)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"[write] {args.out}")

if __name__ == "__main__":
    main()
