*******************************************************
* 04_jolts_import_merge.do — import JOLTS & (optionally) merge
* Assumes: data/jolts_rates_by_industry.csv built by jolts_api.py
* Input panel: results/cps_onet_merged.dta
*******************************************************
version 15.0
clear all
set more off

* ---- Import JOLTS ----
local JOLTS_IN "$dat/jolts_rates_by_industry.csv"
capture confirm file "`JOLTS_IN'"
if _rc {
    di as error "X JOLTS file not found: `JOLTS_IN' (run jolts_api.py from master)"
    exit 601
}

import delimited using "`JOLTS_IN'", clear varnames(1) case(lower) stringcols(_all)
rename *, lower

* Expected minimal cols from our API script: industry, metric, month, value
foreach v in industry metric month value {
    capture confirm variable `v'
    if _rc {
        di as error "X missing column `v' in JOLTS CSV — check jolts_api.py output."
        exit 198
    }
}

* Parse YYYY-MM string to Stata monthly date
gen double mdate = monthly(month, "YM")
format mdate %tm
order industry metric month mdate value

tempfile jlt
save `jlt', replace

* ---- Load CPS+O*NET panel ----
use "$res/cps_onet_merged.dta", clear
rename asecwt wgt
capture confirm numeric variable wgt
if _rc destring wgt, replace force

* Try to merge if a key exists (e.g., ind↔industry or naics2)
local did_merge = 0

* Case 1: CPS has 'ind' (Census industry code) and JOLTS has same numeric key
capture confirm variable ind
if !_rc {
    capture confirm numeric variable ind
    if _rc destring ind, replace force
    tempvar ind_str
    gen str ind_str = string(ind)
    * If JOLTS 'industry' is string, try direct match on text labels
    capture confirm string variable industry using `jlt'
    if !_rc {
        * no robust map yet — skip hard merge to avoid false joins
        di as txt "note: found CPS 'ind', but JOLTS 'industry' is string — leaving unmerged context."
    }
    else {
        * (If both numeric & aligned, you can replace with an actual merge here)
    }
}

* Save pass-through (no destructive join by default)
save "$res/cps_onet_jolts.dta", replace
di as result "saved: $res/cps_onet_jolts.dta (JOLTS kept as separate context in `jlt')"

* Optional: write a small JOLTS summary for reference
preserve
use `jlt', clear
collapse (mean) value, by(metric)
export delimited using "$res/jolts_metric_means.csv", replace
restore
