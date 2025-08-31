*******************************************************
* 02_occ_to_soc.do — OCC -> SOC (stable + safe merge)
* Inputs:
*   $clean/cps_slim.dta
*   $dat/cps_occ_to_soc.csv
* Outputs:
*   $clean/occ2soc_map.dta, $clean/occ2soc_map.csv
*   $clean/cps_soc.dta
*   $clean/unmatched_occ_by_weight.csv (diagnostic)
*******************************************************
version 15.0
clear all
set more off
cap mkdir "$clean"

* -------- paths --------
local CPS_SLIM "$clean/cps_slim.dta"
local XWALK    "$dat/cps_occ_to_soc.csv"

* -------- preflight --------
capture confirm file "`CPS_SLIM'"
if _rc exit 601
capture confirm file "`XWALK'"
if _rc exit 459

* ==============================
* STEP 1. CPS normalize
* ==============================
use "`CPS_SLIM'", clear

capture confirm variable wgt
if _rc {
    capture confirm variable asecwt
    if !_rc gen double wgt = asecwt
    if _rc  gen double wgt = 1
}

capture confirm string variable occ
if _rc {
    destring occ, gen(__o) force
    drop occ
    gen str4 occ = string(__o, "%04.0f")
    drop __o
}
replace occ = trim(occ)
tempvar __o2
destring occ, gen(`__o2') force
replace occ = string(`__o2', "%04.0f") if !missing(`__o2')
drop `__o2'

drop if missing(occ)
drop if occ=="0000"

tempfile cps
save `cps'

preserve
collapse (sum) wgt, by(occ)
tempfile cps_occw
save `cps_occw'
restore

* ==============================
* STEP 2. Crosswalk -> clean map (.dta + .csv)
* ==============================
import delimited using "`XWALK'", varnames(1) stringcols(_all) clear
rename *, lower
ds
local v1 : word 1 of `r(varlist)'
local v2 : word 2 of `r(varlist)'
if "`v1'"!="occ" rename `v1' occ
if "`v2'"!="soc" rename `v2' soc
keep occ soc

* stringify + trim
capture confirm string variable occ
if _rc tostring occ, replace force
capture confirm string variable soc
if _rc tostring soc, replace force
replace occ = trim(occ)
replace soc = trim(soc)

* OCC: pad to 4 digits; leave SOC exactly as provided
tempvar __o
destring occ, gen(`__o') force
replace occ = string(`__o', "%04.0f") if !missing(`__o')
drop `__o'

drop if missing(occ) | missing(soc)
drop if occ=="0000"
quietly count
if r(N)>0 duplicates drop occ soc, force
bysort occ (soc): keep if _n==1

* Save map both ways; for safe-merge, rename soc->soc_map
rename soc soc_map
save "$clean/occ2soc_map.dta", replace
export delimited occ soc_map using "$clean/occ2soc_map.csv", replace

tempfile occmap
save `occmap', replace

* ==============================
* STEP 3. Merge + diagnostics
* ==============================
use `cps', clear

* ensure no name collision; if CPS ever had “soc”, drop it
capture confirm variable soc
if !_rc drop soc

merge m:1 occ using `occmap', keep(master match) keepusing(soc_map)

quietly count if _merge==3
local n_match = r(N)
quietly count if _merge==1
local n_unmatched = r(N)
quietly count
local n_total = r(N)

quietly summarize wgt if _merge==3
local w_match = r(sum)
quietly summarize wgt
local w_total = r(sum)

* finalize SOC and clean
gen str7 soc = soc_map
drop soc_map _merge
order occ soc wgt statefip age sex
save "$clean/cps_soc.dta", replace

di as res "saved: $clean/cps_soc.dta"
di as txt "matched persons=" as res `n_match' "  unmatched=" `n_unmatched' "  total=" `n_total'
di as txt "matched weight share=" as res 100*`w_match'/`w_total' "%"

* ==============================
* STEP 4. Top unmatched OCCs by weight (diagnostic)
* ==============================
preserve
use "$clean/cps_slim.dta", clear
capture confirm variable wgt
if _rc {
    capture confirm variable asecwt
    if !_rc gen double wgt = asecwt
    if _rc  gen double wgt = 1
}
capture confirm string variable occ
if _rc {
    destring occ, gen(__o) force
    drop occ
    gen str4 occ = string(__o, "%04.0f")
    drop __o
}
replace occ = trim(occ)
drop if occ=="0000"
merge m:1 occ using "$clean/occ2soc_map.dta"
keep if _merge==1
collapse (sum) wgt, by(occ)
gsort -wgt
list occ wgt in 1/15, noobs
export delimited using "$clean/unmatched_occ_by_weight.csv", replace
restore
*******************************************************
* end 02_occ_to_soc.do
*******************************************************
