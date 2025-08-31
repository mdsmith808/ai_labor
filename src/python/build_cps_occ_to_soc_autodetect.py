#!/usr/bin/env python3
"""
build_cps_occ_to_soc_autodetect.py

Reads a Census 2018 OCC → 2018 SOC crosswalk Excel and writes a strict two-column CSV:
  occ (4-digit, zero-padded), soc (NN-NNNN).

Robustness upgrades:
- Scans ALL sheets and chooses the best by CONTENT (not just header names).
- Detects OCC/SOC columns by value patterns:
    * OCC: mostly 4-digit numeric codes (e.g., 0010, 6130)
    * SOC: tokens like '12-3456' or '123456' (converts to '12-3456')
- STRICT mode (default): drops rows with multi-SOC cells and drops OCCs that map to >1 SOC.
- EXPAND mode (--allow-multi-soc): expands multi-SOC cells; downstream Stata can choose dominant.

Usage:
  python build_cps_occ_to_soc_autodetect.py \
      --xlsx data/2018_crosswalk.xlsx --out data/cps_occ_to_soc.csv --verbose
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd


# ----------------- pattern helpers (value-level) -----------------

DELIMS_RE = re.compile(r"[;,/|&]| and ", re.IGNORECASE)

def split_tokens(val: str) -> list[str]:
    """Split a cell into tokens on common delimiters; single token if none found."""
    s = ("" if pd.isna(val) else str(val)).strip()
    return [t.strip() for t in DELIMS_RE.split(s) if t.strip()] if DELIMS_RE.search(s) else ([s] if s else [])


SOC_SINGLE_RE = re.compile(r"^\d{2}-\d{4}$")
SOC_DIGITS_RE = re.compile(r"^\d{6}$")

def is_soc_token_like(tok: str) -> bool:
    """True if token looks like a SOC code (NN-NNNN or 6 digits)."""
    s = tok.replace(" ", "")
    return bool(SOC_SINGLE_RE.fullmatch(s) or SOC_DIGITS_RE.fullmatch(s))

def normalize_soc_token(tok: str) -> str | None:
    """Normalize a single token to 'NN-NNNN'; return None if not SOC-like."""
    s = tok.replace(" ", "")
    if SOC_SINGLE_RE.fullmatch(s):
        return s
    if SOC_DIGITS_RE.fullmatch(s):
        return f"{s[:2]}-{s[2:]}"
    return None

def is_occ_like(val: str) -> bool:
    """True if value looks like a (mostly) pure numeric OCC (4-digit once normalized)."""
    if pd.isna(val):
        return False
    s = str(val).strip()
    # reject letters, hyphens, commas etc. (allow trailing .0 from Excel)
    if re.search(r"[^\d\.]", s):
        return False
    try:
        n = int(float(s))
    except Exception:
        return False
    return 0 <= n <= 9999

def normalize_occ(val: str) -> str | None:
    """Normalize value to 4-digit zero-padded occ or None if not parseable."""
    if not is_occ_like(val):
        return None
    n = int(float(str(val)))
    return f"{n:04d}"


# --------------- column- and sheet-level detection ----------------

def column_score_occ(series: pd.Series, max_sample: int = 400) -> float:
    """Fraction of non-null sampled values that are OCC-like."""
    vals = series.dropna().astype(str)
    if vals.empty:
        return 0.0
    sample = vals.head(max_sample)
    good = sum(is_occ_like(v) for v in sample)
    return good / len(sample)

def column_score_soc(series: pd.Series, max_sample: int = 400) -> float:
    """Fraction of non-null sampled values that contain at least one SOC-like token."""
    vals = series.dropna().astype(str)
    if vals.empty:
        return 0.0
    sample = vals.head(max_sample)
    def any_soc(v: str) -> bool:
        toks = split_tokens(v)
        return any(is_soc_token_like(t) for t in toks)
    good = sum(any_soc(v) for v in sample)
    return good / len(sample)

def detect_occ_soc_columns(df: pd.DataFrame, verbose: bool = False) -> Tuple[str | None, str | None, float, float]:
    """
    Score every column for OCC-likeness and SOC-likeness; return the best pair.
    Returns (occ_col, soc_col, occ_score, soc_score) where columns can be None.
    """
    if df.empty or df.shape[1] == 0:
        return None, None, 0.0, 0.0

    # pre-trim whitespace strings
    for c in df.columns:
        if pd.api.types.is_string_dtype(df[c]):
            df[c] = df[c].astype(str).str.strip()

    occ_best, soc_best = None, None
    occ_best_score, soc_best_score = 0.0, 0.0

    # score all columns
    occ_scores = {}
    soc_scores = {}
    for c in df.columns:
        try:
            occ_scores[c] = column_score_occ(df[c])
            soc_scores[c] = column_score_soc(df[c])
        except Exception:
            occ_scores[c] = 0.0
            soc_scores[c] = 0.0

    # pick best distinct columns
    occ_best = max(occ_scores, key=lambda k: occ_scores[k]) if occ_scores else None
    soc_best = max(soc_scores, key=lambda k: soc_scores[k]) if soc_scores else None
    occ_best_score = occ_scores.get(occ_best, 0.0) if occ_best else 0.0
    soc_best_score = soc_scores.get(soc_best, 0.0) if soc_best else 0.0

    # avoid same column for both; pick second-best for SOC if needed
    if occ_best is not None and soc_best == occ_best:
        # remove the shared column from SOC candidates and re-pick
        soc_candidates = {k: v for k, v in soc_scores.items() if k != occ_best}
        if soc_candidates:
            soc_best = max(soc_candidates, key=lambda k: soc_candidates[k])
            soc_best_score = soc_candidates[soc_best]
        else:
            soc_best, soc_best_score = None, 0.0

    if verbose:
        print(f"[detect] occ_col={repr(occ_best)} score={occ_best_score:.3f}, "
              f"soc_col={repr(soc_best)} score={soc_best_score:.3f}", file=sys.stderr)

    return occ_best, soc_best, occ_best_score, soc_best_score

def pick_best_sheet(xlsx: Path, sheet_arg: str | None, verbose: bool = False) -> Tuple[str, pd.DataFrame]:
    """
    Pick the sheet with the strongest OCC & SOC signals by content.
    If sheet_arg provided, use it.
    """
    if sheet_arg:
        df = pd.read_excel(xlsx, sheet_name=sheet_arg, engine="openpyxl", dtype=str)
        if verbose:
            print(f"[io] using requested sheet: {sheet_arg}", file=sys.stderr)
        return sheet_arg, df

    all_sheets = pd.read_excel(xlsx, sheet_name=None, engine="openpyxl", dtype=str)
    best_name, best_df, best_combo = None, None, -1.0
    for name, df in all_sheets.items():
        occ_col, soc_col, os, ss = detect_occ_soc_columns(df.copy(), verbose=False)
        combo = os * ss  # both must be non-trivial
        if combo > best_combo:
            best_combo, best_name, best_df = combo, name, df

    if best_df is None:
        # fallback to first arbitrarily (but will error later)
        name = next(iter(all_sheets))
        if verbose:
            print(f"[warn] no sheet had detectable columns; falling back to: {name}", file=sys.stderr)
        return name, all_sheets[name]

    if verbose:
        print(f"[io] auto-selected sheet: {best_name} (score={best_combo:.3f})", file=sys.stderr)
    return best_name, best_df


# ----------------- build crosswalk -----------------

def build_crosswalk(xlsx: Path,
                    out_csv: Path,
                    sheet: str | None = None,
                    allow_multi_soc: bool = False,
                    verbose: bool = False) -> pd.DataFrame:
    """Construct strict/expanded OCC→SOC crosswalk and write to CSV."""
    sheet_name, df = pick_best_sheet(xlsx, sheet, verbose)
    # Drop fully empty rows
    df = df.dropna(how="all").copy()

    # Detect columns by value-pattern scoring
    occ_col, soc_col, occ_score, soc_score = detect_occ_soc_columns(df.copy(), verbose=True)

    # Require minimum quality; these thresholds are lenient but avoid OVERVIEW sheets
    if not occ_col or not soc_col or occ_score < 0.5 or soc_score < 0.5:
        raise ValueError(f"Could not detect OCC/SOC columns confidently in sheet {sheet_name!r} "
                         f"(scores: occ={occ_score:.2f}, soc={soc_score:.2f}). "
                         f"Try --sheet with the exact sheet name.")

    raw_rows = len(df)
    if verbose:
        print(f"[stats] sheet={sheet_name}, raw_rows={raw_rows}", file=sys.stderr)

    # Keep only chosen columns
    df = df[[occ_col, soc_col]].rename(columns={occ_col: "occ_raw", soc_col: "soc_raw"})

    # Normalize OCC
    df["occ"] = df["occ_raw"].map(normalize_occ)
    df = df[~df["occ"].isna()].copy()
    if verbose:
        print(f"[stats] after OCC normalize: {len(df)} rows", file=sys.stderr)

    # Normalize SOC(s)
    if allow_multi_soc:
        df["soc_list"] = df["soc_raw"].map(lambda v: [normalize_soc_token(t) for t in split_tokens(v)])
        # drop invalid tokens and explode
        df["soc_list"] = df["soc_list"].map(lambda lst: [x for x in lst if x])
        df = df.explode("soc_list", ignore_index=True)
        df = df[df["soc_list"].notna() & (df["soc_list"] != "")]
        df = df.rename(columns={"soc_list": "soc"})
    else:
        # STRICT: only accept single-token cells that normalize cleanly
        def strict_soc(v):
            toks = split_tokens(v)
            if len(toks) != 1:
                return None
            return normalize_soc_token(toks[0])
        df["soc"] = df["soc_raw"].map(strict_soc)
        df = df[~df["soc"].isna()].copy()

    if verbose:
        print(f"[stats] after SOC normalize ({'expand' if allow_multi_soc else 'strict'}): {len(df)} rows", file=sys.stderr)

    # Keep only occ,soc; drop duplicates
    df = df[["occ", "soc"]].dropna().drop_duplicates()

    # STRICT one-to-one: drop OCCs mapping to >1 SOC
    dup_occ_mask = df.duplicated(subset=["occ"], keep=False)
    ambiguous_occs = df.loc[dup_occ_mask, "occ"].unique().tolist()
    if ambiguous_occs:
        if verbose:
            print(f"[strict] dropping {len(ambiguous_occs)} ambiguous OCCs with multiple SOCs", file=sys.stderr)
        df = df[~df["occ"].isin(ambiguous_occs)].copy()

    final_rows = len(df)
    if verbose:
        print(f"[stats] final strict pairs: {final_rows}", file=sys.stderr)

    # Write output
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    if verbose:
        print(f"[io] wrote → {out_csv} (rows={final_rows})", file=sys.stderr)

    # Basic warn if suspiciously tiny
    if final_rows < 100:
        print(f"[warn] crosswalk is small (rows={final_rows}). Check the chosen sheet/columns.", file=sys.stderr)

    return df


# ----------------- CLI -----------------

def main():
    ap = argparse.ArgumentParser(description="Build a strict OCC→SOC crosswalk CSV from a messy Excel file.")
    ap.add_argument("--xlsx", required=True, type=Path, help="Path to 2018 crosswalk .xlsx")
    ap.add_argument("--out", required=True, type=Path, help="Output CSV path (occ,soc)")
    ap.add_argument("--sheet", default=None, help="Excel sheet name (default: auto-detect best)")
    ap.add_argument("--allow-multi-soc", action="store_true", help="Expand multi-SOC cells (NOT strict)")
    ap.add_argument("--verbose", action="store_true", help="Print details to stderr")
    args = ap.parse_args()

    if not args.xlsx.exists():
        print(f"[error] Excel input not found: {args.xlsx}", file=sys.stderr)
        sys.exit(2)

    try:
        build_crosswalk(args.xlsx, args.out, sheet=args.sheet,
                        allow_multi_soc=args.allow_multi_soc, verbose=args.verbose)
    except Exception as e:
        print(f"[error] failed to build crosswalk: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
