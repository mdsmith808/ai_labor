*******************************************************
* 02a_clean_crosswalk.do — build a clean OCC→SOC map
* Input:  $dat/2018-occupation-code-list-and-crosswalk.xlsx
* Sheet: tries variants of "2010 to 2018 Crosswalk"
* Output: $dat/cps_occ_to_soc.csv  (occ,soc)
* Copies: $clean/occ2soc_map.csv, $clean/xwalk_clean.dta
*******************************************************
version 15.0
clear all
set more off

* expected globals set in 00_master_api15.do
*   global proj "/Users/mdsmith808/research/ai_labor"
*   global dat  "$proj/data"
*   global clean "$proj/data/clean"
cap mkdir "$clean"

local XWALK_XLSX "$dat/2018-occupation-code-list-and-crosswalk.xlsx"
capture confirm file "`XWALK_XLSX'"
if _rc {
    di as error "X missing: `XWALK_XLSX'"
    exit 601
}

* ---------- STEP 1: import the crosswalk sheet ----------
local done 0
local picked ""

* try these common sheet-name variants
foreach S in "2010 to 2018 Crosswalk" "2010 to 2018 crosswalk" "2010 to 2018 Crosswalk " {
    capture noisily import excel using "`XWALK_XLSX'", ///
        sheet("`S'") firstrow allstring clear
    if !_rc {
        quietly count
        if r(N)>0 {
            local done 1
            local picked "`S'"
            continue, break
        }
    }
}
* if still not found, try starting at later rows (header offset)
if `done'==0 {
    foreach S in "2010 to 2018 Crosswalk" "2010 to 2018 crosswalk" "2010 to 2018 Crosswalk " {
        foreach R in 2 3 4 5 6 {
            capture noisily import excel using "`XWALK_XLSX'", ///
                sheet("`S'") cellrange(A`R':Z5000) firstrow allstring clear
            if !_rc {
                quietly count
                if r(N)>0 {
                    local done 1
                    local picked "`S' (start row `R')"
                    continue, break
                }
            }
        }
        if `done'==1 continue, break
    }
}
if `done'==0 {
    di as error "X could not import the crosswalk sheet from: `XWALK_XLSX'"
    exit 459
}
rename *, lower
di as txt "✓ imported sheet: " as res "`picked'"

* ---------- STEP 2: detect OCC and SOC columns by content ----------
tempname best_occ best_soc
scalar `best_occ' = .
scalar `best_soc' = .
local occvar ""
local socvar ""

ds
local VLIST `r(varlist)'
foreach v of local VLIST {
    tempvar s
    gen str80 `s' = trim(`v')
    quietly count if !missing(`s')
    local tot = r(N)

    local sc_occ 0
    local sc_soc 0
    if `tot' > 0 {
        quietly count if regexm(`s',"^[0-9]{1,4}$")
        local sc_occ = 100*r(N)/`tot'
        quietly count if regexm(`s',"^[0-9]{6}$") | regexm(`s',"^[0-9]{2}[^0-9]+[0-9]{4}$")
        local sc_soc = 100*r(N)/`tot'
    }
    if missing(scalar(`best_occ')) | (`sc_occ' > scalar(`best_occ')) {
        scalar `best_occ' = `sc_occ'
        local occvar "`v'"
    }
    if missing(scalar(`best_soc')) | (`sc_soc' > scalar(`best_soc')) {
        scalar `best_soc' = `sc_soc'
        local socvar "`v'"
    }
    drop `s'
}

* ensure the two columns are distinct; if not, pick next-best SOC
if "`occvar'"=="`socvar'" {
    scalar `best_soc' = .
    local soc2 ""
    foreach v of local VLIST {
        if "`v'"=="`occvar'" continue
        tempvar s2
        gen str80 `s2' = trim(`v')
        quietly count if !missing(`s2')
        local tot = r(N)
        local sc 0
        if `tot'>0 {
            quietly count if regexm(`s2',"^[0-9]{6}$") | regexm(`s2',"^[0-9]{2}[^0-9]+[0-9]{4}$")
            local sc = 100*r(N)/`tot'
        }
        if missing(scalar(`best_soc')) | (`sc' > scalar(`best_soc')) {
            scalar `best_soc' = `sc'
            local soc2 "`v'"
        }
        drop `s2'
    }
    local socvar "`soc2'"
}

if "`occvar'"=="" | "`socvar'"=="" {
    di as error "X failed to detect distinct OCC/SOC columns."
    di as txt "vars: `VLIST'"
    exit 459
}
di as txt "→ OCC col: " as res "`occvar'" as txt " | SOC col: " as res "`socvar'"

keep `occvar' `socvar'
rename `occvar' occ
rename `socvar' soc

* ---------- STEP 3: normalize ----------
capture confirm string variable occ
if _rc tostring occ, replace force
capture confirm string variable soc
if _rc tostring soc, replace force
replace occ = trim(occ)
replace soc = trim(soc)

* OCC: digits only → 4-digit (rightmost 4 to be safe)
gen str20 __occdig = occ
replace __occdig = subinstr(__occdig,"-","",.)
replace __occdig = subinstr(__occdig,"/","",.)
replace __occdig = subinstr(__occdig," ","",.)
replace __occdig = subinstr(__occdig,".","",.)
replace __occdig = subinstr(__occdig,char(160),"",.)
gen str4 occ_clean = substr(__occdig, max(1, length(__occdig)-3), 4)
drop __occdig
replace occ = occ_clean if occ_clean!=""
drop occ_clean

* SOC: digits only → NN-NNNN (first 6 digits)
gen str20 __socdig = soc
replace __socdig = subinstr(__socdig,"-","",.)
replace __socdig = subinstr(__socdig,"/","",.)
replace __socdig = subinstr(__socdig," ","",.)
replace __socdig = subinstr(__socdig,".","",.)
replace __socdig = subinstr(__socdig,char(160),"",.)
gen str7 soc_norm = substr(__socdig,1,2) + "-" + substr(__socdig,3,4) if length(__socdig)>=6
drop __socdig
replace soc = soc_norm if soc_norm!=""
drop soc_norm

drop if missing(occ) | missing(soc)
drop if !regexm(occ,"^[0-9]{4}$")
drop if !regexm(soc,"^[0-9]{2}-[0-9]{4}$")

capture quietly duplicates drop occ soc, force
bysort occ (soc): keep if _n==1

quietly count
di as txt "✓ clean xwalk rows = " as res r(N)
if r(N)==0 {
    di as error "X crosswalk ended empty after cleaning."
    exit 459
}

* ---------- STEP 4: write outputs ----------
export delimited occ soc using "$dat/cps_occ_to_soc.csv", replace
di as result "wrote → $dat/cps_occ_to_soc.csv"
export delimited occ soc using "$clean/occ2soc_map.csv", replace
save "$clean/xwalk_clean.dta", replace

di as result "also wrote → $clean/occ2soc_map.csv and $clean/xwalk_clean.dta"
*******************************************************
* end 02a_clean_crosswalk.do
*******************************************************
