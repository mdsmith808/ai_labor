*******************************************************
* 00_master_api15.do — API-first build (Stata 15)
* CPS via IPUMS API → OCC→SOC → O*NET → exposure
*******************************************************
version 15.0
clear all
set more off

* project globals (lowercase)
global proj "`c(pwd)'"          // run this from the project root
global dat  "$proj/data"
global res  "$proj/results"
global docs "$proj/docs"

cap mkdir "$dat"
cap mkdir "$res"
cap mkdir "$res/figures"
cap mkdir "$res/logs"

* prefer venv python; fall back to system
local py "./.venv/bin/python"
capture noisily shell `py' -V
if _rc local py "python"

/*
cap log close
log using "$res/logs/00_master_api15.smcl", replace

*******************************************************
* 1) CPS via IPUMS API (reads .env: IPUMS_API_KEY or IPUMS_KEY)
*******************************************************
local cps_samples "cps2024_03s"     // ASEC March 2024
local cps_vars    "OCC ASECWT STATEFIP AGE SEX"

shell "`py'" "src/python/pull_cps_api.py" ///
    --samples `cps_samples' ///
    --vars `cps_vars' ///
    --out "$dat/cps_asec_2024.csv"

*******************************************************
* 2) Ensure crosswalk (occ→soc) exists (auto-build from local xlsx if present)
*******************************************************
capture confirm file "$dat/cps_occ_to_soc.csv"
if _rc {
    capture confirm file "$dat/2018_crosswalk.xlsx"
    if !_rc {
        di as txt "crosswalk missing — building from local 2018_crosswalk.xlsx"
        shell "`py'" "src/python/build_cps_occ_to_soc_autodetect.py" ///
            --xlsx "$dat/2018_crosswalk.xlsx" --out "$dat/cps_occ_to_soc.csv"
    }
    else {
        di as error "X missing $dat/cps_occ_to_soc.csv (and no 2018_crosswalk.xlsx to build from)."
        exit 601
    }
}

*******************************************************
* 3) Setup (ado installs, defaults)
*******************************************************
do "src/stata/utils/00_setup.do"

*******************************************************
* 4) CPS ingest (force numeric weight, 4-digit string occ)
*******************************************************
do "src/stata/01_cps_ingest.do"

*******************************************************
* 5) OCC -> SOC (dominant SOC by weight)
*******************************************************
do "src/stata/02_occ_to_soc.do"

*******************************************************
* 6) O*NET join & exposure v0.1
*******************************************************
shell "`py'" "src/python/onet_fetch_parse.py" "$dat/onet_db.zip" "$dat/onet_tasks_soc.csv"
do "src/stata/03_onet_join_exposure.do"

* (rest of pipeline unchanged)
* shell "`py'" "src/python/jolts_api.py" "$dat/jolts_series_map.csv" "$dat/jolts_rates_by_industry.csv" 2019 2025
* do "src/stata/04_jolts_import_merge.do"
* shell "`py'" "src/python/lodes_fetch.py" "ca" "2019" "wac" "$dat/lehd_flows.csv"
* do "src/stata/05_lehd_import_merge.do"
* do "src/stata/06_snapshot_and_figs.do"

log close
display as result "✅ master done — see $res and $res/figures/"
