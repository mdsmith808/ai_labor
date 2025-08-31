*******************************************************
* 01_cps_ingest.do — load CPS CSV → slim, typed vars
* Stata 15+; robust to name/type quirks and OCC collisions
*******************************************************
version 15.0
clear all
set more off

* ---------- paths (from master) ----------
* expects globals like: global dat "/path/to/data", global res "/path/to/results"
local CPS_IN "$dat/cps_asec_2024.csv"
* ----------------------------------------

* ---------- preflight ----------
capture confirm file "`CPS_IN'"
if _rc {
    di as error "X input file not found: `CPS_IN'"
    exit 601
}

* Import with all columns as strings first, then coerce selected vars explicitly
import delimited using "`CPS_IN'", clear varnames(1) case(lower) stringcols(_all)

* ---------- choose occupation source (first-found wins) ----------
local occsrc ""
foreach cand in occ occ2018 occ2010 occ1990 {
    capture confirm variable `cand'
    if !_rc {
        local occsrc "`cand'"
        continue, break
    }
}
if "`occsrc'" == "" {
    di as error "X no occupation column found (occ/occ2018/occ2010/occ1990)."
    exit 198
}

* If the chosen source is named 'occ', move it aside to avoid conflict with target 'occ'
* If some OTHER 'occ' already exists, move it aside as well.
if "`occsrc'" == "occ" {
    capture drop __occ_src
    rename occ __occ_src
    local occsrc "__occ_src"
}
else {
    capture confirm variable occ
    if !_rc {
        capture drop occ_src_old
        rename occ occ_src_old
    }
}

* ---------- build canonical 4-digit string OCC ----------
tempvar occnum
quietly destring `occsrc', gen(`occnum') force
* drop rows where OCC could not be parsed into a number
drop if missing(`occnum')

gen str4 occ = string(`occnum', "%04.0f")
label var occ "Occupation (4-digit string, zero-padded)"

* ---------- ensure numeric person weight named asecwt ----------
* Prefer IPUMS ASECWT; fall back to WTFINL if present
capture confirm variable asecwt
if _rc {
    capture confirm variable wtfinl
    if _rc {
        di as error "X missing weight (asecwt/wtfinl)."
        exit 198
    }
    rename wtfinl asecwt
}

* Coerce to numeric if string
capture confirm numeric variable asecwt
if _rc quietly destring asecwt, replace force

* Validate numeric
capture confirm numeric variable asecwt
if _rc {
    di as error "X weight is not numeric after destring."
    exit 198
}
label var asecwt "ASEC person weight"

* ---------- optional keepers (create if absent; coerce to numeric if present as string) ----------
foreach v in statefip age sex {
    capture confirm variable `v'
    if _rc {
        gen `v' = .
    }
    else {
        capture confirm numeric variable `v'
        if _rc quietly destring `v', replace force ignore(" ")
        capture confirm numeric variable `v'
        if _rc {
            drop `v'
            gen `v' = .
        }
    }
}

label var statefip "State (FIPS)"
label var age      "Age"
label var sex      "Sex"

* ---------- light diagnostics (non-fatal) ----------
quietly count if missing(occ)
if r(N) di as txt "i note: `r(N)' rows have missing occ (after parsing) — retained."

quietly count if missing(asecwt)
if r(N) di as txt "i note: `r(N)' rows have missing asecwt — retained."

* ---------- finalize slim schema ----------
keep occ asecwt statefip age sex
order occ asecwt statefip age sex
compress

* ---------- save ----------
capture confirm file "$res"
* (no-op: just in case $res is a directory macro from master)

save "$clean/cps_slim.dta", replace
di as result "saved: $clean/cps_slim.dta"

* ---------- cleanup scratch ----------
capture drop __occ_src occ_src_old
*******************************************************
* end 01_cps_ingest.do
*******************************************************
