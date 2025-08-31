*******************************************************
* 03_onet_join_exposure.do — O*NET join & exposure v0.1
*******************************************************
version 15.0
clear all
set more off

* onet tasks csv (built by onet_fetch_parse.py)
local ONET_IN "$dat/onet_tasks_soc.csv"

* 1) O*NET → SOC exposure
import delimited using "`ONET_IN'", clear varnames(1) stringcols(_all)
rename *, lower
foreach v in soc task_importance task_time {
    capture confirm variable `v'
    if _rc {
        di as error "X missing `v' in `ONET_IN'"
        exit 198
    }
}
destring task_importance task_time, replace force

sum task_importance
local imp_norm = cond(r(max)>1, 100, 1)
gen double __imp  = task_importance/`imp_norm'

sum task_time
local time_norm = cond(r(max)>1, 100, 1)
gen double __time = task_time/`time_norm'

gen double __align = __imp * __time
collapse (sum) __align __time, by(soc)
gen double exposure_v01 = __align / __time
keep soc exposure_v01
tempfile onet_soc
save `onet_soc', replace

* 2) merge to CPS (with SOC) & export snapshots
use "$res/cps_soc.dta", clear
rename asecwt wgt
capture confirm numeric variable wgt
if _rc destring wgt, replace force

* soc should be string
capture confirm string variable soc
if _rc tostring soc, replace force

merge m:1 soc using `onet_soc', keep(master match) nogen

* SOC-level weighted snapshot (CPS weights)
preserve
keep soc exposure_v01 wgt
collapse (mean) exposure_v01 [pw=wgt], by(soc)
xtile exposure_pct = exposure_v01, n(100)
order soc exposure_v01 exposure_pct
export delimited using "$res/exposure_snapshot.csv", replace
restore

save "$res/cps_onet_merged.dta", replace
di as result "saved: $res/exposure_snapshot.csv & $res/cps_onet_merged.dta"
