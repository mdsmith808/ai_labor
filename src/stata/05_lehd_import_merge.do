*******************************************************
* 05_lehd_import_merge.do — import LODES & (optionally) merge
* Assumes: data/lehd_flows.csv created by lodes_fetch.py
* Input panel: results/cps_onet_jolts.dta
*******************************************************
version 15.0
clear all
set more off

local LODES_IN "$dat/lehd_flows.csv"
capture confirm file "`LODES_IN'"
if _rc {
    di as error "X LODES file not found: `LODES_IN' (run lodes_fetch.py from master)"
    exit 601
}

import delimited using "`LODES_IN'", clear varnames(1) case(lower) stringcols(_all)
rename *, lower

* Try to standardize a state key as numeric statefip if present
tempvar stnum
capture confirm variable statefip
if !_rc {
    capture confirm numeric variable statefip
    if _rc destring statefip, replace force
    gen double `stnum' = statefip
}
else {
    * common LODES fields: st or state (as 2-letter). We won't guess → keep as context only.
    di as txt "note: no numeric statefip detected in LODES — leaving unmerged context."
}

tempfile lhd
save `lhd', replace

* Load CPS panel (with JOLTS pass-through)
use "$res/cps_onet_jolts.dta", clear

* Optional merge if both sides have numeric statefip
local did_merge = 0
capture confirm numeric variable statefip
if !_rc {
    tempname __mergeok
    scalar `__mergeok' = 0
    quietly use `lhd', clear
    capture confirm numeric variable statefip
    if !_rc scalar `__mergeok' = 1
    use "$res/cps_onet_jolts.dta", clear
    if (scalar(`__mergeok')==1) {
        merge m:1 statefip using `lhd', keep(master match) nogen
        local did_merge = 1
    }
}

if `did_merge'==1 {
    di as result "merged LODES on statefip → saved: $res/analysis_panel.dta"
    save "$res/analysis_panel.dta", replace
}
else {
    di as txt "note: kept LODES as separate context; writing analysis panel without LODES merge."
    save "$res/analysis_panel.dta", replace
}
